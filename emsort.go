// see https://en.wikipedia.org/wiki/External_sorting#External_merge_sort
// see http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
package emsort

import (
	"bufio"
	"container/heap"
	"fmt"
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

// SortedWriter is an io.WriteCloser that sorts its output on writing. Each
// []byte passed to the Write method is treated as a single item to sort. Since
// these []byte are kept in memory, they must not be pooled/shared!
type SortedWriter interface {
	Write(b []byte) (int, error)

	Close() error
}

// Less is a function that compares two byte arrays and determines whether a is
// less than b.
type Less func(a []byte, b []byte) bool

// Chunk is a function that chunks data read from the given io.Reader into items
// for sorting.
type Chunk func(io.Reader) ([]byte, error)

func New(out io.Writer, chunk Chunk, less Less, memLimit int) (SortedWriter, error) {
	tmpDir, err := ioutil.TempDir("", "emsort")
	if err != nil {
		return nil, err
	}

	return &sorted{
		tmpDir:   tmpDir,
		out:      out,
		chunk:    chunk,
		less:     less,
		memLimit: memLimit,
	}, nil
}

type sorted struct {
	tmpDir   string
	out      io.Writer
	chunk    Chunk
	less     Less
	memLimit int
	memUsed  int
	numFiles int
	vals     [][]byte
}

func (s *sorted) Write(b []byte) (int, error) {
	s.vals = append(s.vals, b)
	s.memUsed += len(b)
	if s.memUsed >= s.memLimit {
		flushErr := s.flush()
		if flushErr != nil {
			return 0, flushErr
		}
	}
	return len(b), nil
}

func (s *sorted) flush() error {
	sort.Sort(&inmemory{s.vals, s.less})
	file, err := os.OpenFile(filepath.Join(s.tmpDir, strconv.Itoa(s.numFiles)), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	s.numFiles++
	s.memUsed = 0
	out := bufio.NewWriterSize(file, 65536)
	for _, val := range s.vals {
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
	s.vals = s.vals[:0]
	return nil
}

func (s *sorted) Close() error {
	defer os.RemoveAll(s.tmpDir)

	if s.memUsed > 0 {
		flushErr := s.flush()
		if flushErr != nil {
			return flushErr
		}
	}

	entries := &entryHeap{less: s.less}
	files := make(map[int]*bufio.Reader, s.numFiles)
	for i := 0; i < s.numFiles; i++ {
		file, err := os.OpenFile(filepath.Join(s.tmpDir, strconv.Itoa(i)), os.O_RDONLY, 0)
		if err != nil {
			return fmt.Errorf("Unable to open temp file: %v", err)
		}
		defer file.Close()
		files[i] = bufio.NewReaderSize(file, 65536)
	}

	perFileLimit := s.memLimit / (s.numFiles + 1)
	fillBuffer := func() error {
		for i := 0; i < len(files); i++ {
			file := files[i]
			amountRead := 0
			for {
				b, err := s.chunk(file)
				if err == io.EOF {
					delete(files, i)
					break
				}
				if err != nil {
					return fmt.Errorf("Error filling buffer: %v", err)
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

	for {
		if len(entries.entries) == 0 {
			fillErr := fillBuffer()
			if fillErr != nil {
				return fillErr
			}
		}
		if len(entries.entries) == 0 {
			// Nothing left with which to fill buffer, stop
			break
		}
		_e := heap.Pop(entries)
		e := _e.(*entry)
		_, writeErr := s.out.Write(e.val)
		if writeErr != nil {
			return fmt.Errorf("Error writing to final output: %v", writeErr)
		}
		file := files[e.fileIdx]
		if file != nil {
			b, err := s.chunk(file)
			if err == io.EOF {
				delete(files, e.fileIdx)
				continue
			}
			if err != nil {
				return fmt.Errorf("Error replacing entry on heap: %v", err)
			}
			heap.Push(entries, &entry{e.fileIdx, b})
		}
	}

	switch c := s.out.(type) {
	case io.Closer:
		return c.Close()
	default:
		return nil
	}
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
