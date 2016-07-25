package emsort

import (
	"bufio"
	"container/heap"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

type Data interface {
	Fill(func([]byte) error) error

	Read(io.Reader) ([]byte, error)

	Write(io.Writer, []byte) error

	Less(a, b []byte) bool
}

// see https://en.wikipedia.org/wiki/External_sorting#External_merge_sort
// see http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
func Sorted(data Data, memLimit int) (string, error) {
	tmpDir, err := ioutil.TempDir("", "emsort")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)

	numFiles := 0
	memUsed := 0
	var vals [][]byte

	flush := func() error {
		sort.Sort(&inmemory{vals, data.Less})
		file, err := os.OpenFile(filepath.Join(tmpDir, strconv.Itoa(numFiles)), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		numFiles++
		memUsed = 0
		out := bufio.NewWriterSize(file, 65536)
		for _, val := range vals {
			writeErr := data.Write(file, val)
			if writeErr != nil {
				file.Close()
				return writeErr
			}
		}
		err = out.Flush()
		if err != nil {
			file.Close()
			return err
		}
		closeErr := file.Close()
		if closeErr != nil {
			return closeErr
		}
		vals = vals[:0]
		return nil
	}

	fillErr := data.Fill(func(b []byte) error {
		memUsed += len(b)
		if memUsed >= memLimit {
			flushErr := flush()
			if flushErr != nil {
				return flushErr
			}
		}
		return nil
	})
	if fillErr != nil {
		return "", fillErr
	}

	if memUsed > 0 {
		flushErr := flush()
		if flushErr != nil {
			return "", flushErr
		}
	}

	entries := &entryHeap{make([]*entry, 0), data.Less}
	files := make(map[int]*bufio.Reader, numFiles)
	for i := 0; i < numFiles; i++ {
		file, err := os.OpenFile(filepath.Join(tmpDir, strconv.Itoa(i)), os.O_RDONLY, 0)
		if err != nil {
			return "", err
		}
		defer file.Close()
		files[i] = bufio.NewReaderSize(file, 65536)
	}

	perFileLimit := memLimit / (numFiles + 1)
	for i := 0; i < numFiles; i++ {
		file := files[i]
		amountRead := 0
		for {
			b, err := data.Read(file)
			if err == io.EOF {
				delete(files, i)
				break
			}
			if err != nil {
				return "", err
			}
			amountRead += len(b)
			heap.Push(entries, &entry{i, b})
			if amountRead >= perFileLimit {
				break
			}
		}
	}

	out, err := ioutil.TempFile("", "emsort_out")
	if err != nil {
		return "", err
	}
	defer out.Close()
	bout := bufio.NewWriterSize(out, 65536)

	for {
		if len(entries.entries) == 0 {
			break
		}
		_e := heap.Pop(entries)
		e := _e.(*entry)
		writeErr := data.Write(bout, e.val)
		if writeErr != nil {
			return "", err
		}
		file := files[e.fileIdx]
		if file != nil {
			b, err := data.Read(file)
			if err == io.EOF {
				delete(files, e.fileIdx)
				break
			}
			if err != nil {
				return "", err
			}
			heap.Push(entries, &entry{e.fileIdx, b})
		}

		flushErr := bout.Flush()
		if flushErr != nil {
			return "", err
		}
	}

	return out.Name(), nil
}

type inmemory struct {
	vals [][]byte
	less func(a, b []byte) bool
}

func (im *inmemory) Len() int {
	return len(im.vals)
}

func (im *inmemory) Less(i, j int) bool {
	return im.less(im.vals[i], im.vals[j])
}

func (im *inmemory) Swap(i, j int) {
	im.vals[i], im.vals[j] = im.vals[j], im.vals[i]
}

type entry struct {
	fileIdx int
	val     []byte
}

type entryHeap struct {
	entries []*entry
	less    func(a, b []byte) bool
}

func (eh *entryHeap) Len() int {
	return len(eh.entries)
}

func (eh *entryHeap) Less(i, j int) bool {
	return eh.less(eh.entries[i].val, eh.entries[j].val)
}

func (eh *entryHeap) Swap(i, j int) {
	eh.entries[i], eh.entries[j] = eh.entries[j], eh.entries[i]
}

func (eh *entryHeap) Push(x interface{}) {
	eh.entries = append(eh.entries, x.(*entry))
}

func (eh *entryHeap) Pop() interface{} {
	n := len(eh.entries)
	x := eh.entries[n-1]
	eh.entries = eh.entries[:n-1]
	return x
}
