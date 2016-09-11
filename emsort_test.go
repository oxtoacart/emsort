package emsort

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundTripMultipleFiles(t *testing.T) {
	w := &assertingWriter{&bytes.Buffer{}}
	s, err := New(w, chunk, less, 1000)
	doTestRoundTrip(t, w, s, err)
}

func TestRoundTripSingleFile(t *testing.T) {
	w := &assertingWriter{&bytes.Buffer{}}
	s, err := New(w, chunk, less, 100000000)
	doTestRoundTrip(t, w, s, err)
}

func doTestRoundTrip(t *testing.T, w *assertingWriter, s SortedWriter, err error) {
	if assert.NoError(t, err) {
		halfMaxInt := int64(math.MaxInt64 / 2)
		for i := 0; i < 100000; i++ {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(halfMaxInt+(rand.Int63n(halfMaxInt))))
			n, err := s.Write(b)
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, 8, n)
		}
		err := s.Close()
		if !assert.NoError(t, err) {
			return
		}
		w.finish(t)
	}
}

func chunk(r io.Reader) ([]byte, error) {
	b := make([]byte, 8)
	_, err := io.ReadFull(r, b)
	return b, err
}

func less(a []byte, b []byte) bool {
	return bytes.Compare(a, b) < 0
}

type assertingWriter struct {
	buf *bytes.Buffer
}

func (w *assertingWriter) Write(b []byte) (int, error) {
	return w.buf.Write(b)
}

func (w *assertingWriter) finish(t *testing.T) {
	last := int64(-1)
	numResults := 0
	for {
		var next int64
		err := binary.Read(w.buf, binary.BigEndian, &next)
		if err == io.EOF {
			break
		}
		if !assert.NoError(t, err) {
			return
		}
		if !assert.True(t, next > last, fmt.Sprintf("%d not greater than or equal to %d", next, last)) {
			return
		}
		last = next
		numResults++
	}
	assert.Equal(t, 100000, numResults)
}
