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

	Less(a []byte, b []byte) bool

	OnSorted([]byte) error
}

// see https://en.wikipedia.org/wiki/External_sorting#External_merge_sort
// see http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
func Sorted(data Data, memLimit int) error {
	tmpDir, err := ioutil.TempDir("", "emsort")
	if err != nil {
		return err
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
			_, writeErr := file.Write(val)
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
		vals = append(vals, b)
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
		return fillErr
	}

	if memUsed > 0 {
		flushErr := flush()
		if flushErr != nil {
			return flushErr
		}
	}

	entries := &entryHeap{less: data.Less}
	files := make(map[int]*bufio.Reader, numFiles)
	for i := 0; i < numFiles; i++ {
		file, err := os.OpenFile(filepath.Join(tmpDir, strconv.Itoa(i)), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		defer file.Close()
		files[i] = bufio.NewReaderSize(file, 65536)
	}

	perFileLimit := memLimit / (numFiles + 1)
	fillBuffer := func() error {
		for i := 0; i < len(files); i++ {
			file := files[i]
			amountRead := 0
			for {
				b, err := data.Read(file)
				if err == io.EOF {
					delete(files, i)
					break
				}
				if err != nil {
					return err
				}
				amountRead += len(b)
				heap.Push(entries, &entry{i, b})
				if amountRead >= perFileLimit {
					break
				}
			}
		}

		return nil
	}

	err = fillBuffer()
	if err != nil {
		return err
	}

	for {
		if len(entries.entries) == 0 {
			err := fillBuffer()
			if err != nil {
				return err
			}
		}
		if len(entries.entries) == 0 {
			// Nothing left with which to fill buffer, stop
			break
		}
		_e := heap.Pop(entries)
		e := _e.(*entry)
		writeErr := data.OnSorted(e.val)
		if writeErr != nil {
			return err
		}
		file := files[e.fileIdx]
		if file != nil {
			b, err := data.Read(file)
			if err == io.EOF {
				delete(files, e.fileIdx)
				continue
			}
			if err != nil {
				return err
			}
			heap.Push(entries, &entry{e.fileIdx, b})
		}
	}

	return nil
}

type inmemory struct {
	vals [][]byte
	less func(a []byte, b []byte) bool
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
	less    func([]byte, []byte) bool
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
