package emsort

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundTrip(t *testing.T) {
	w := &assertingWriter{t: t}
	s, err := New(w, chunk, less, 1000)
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
		assert.Equal(t, 100000, w.numResults)
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
	t          *testing.T
	last       int64
	numResults int
}

func (w *assertingWriter) Write(b []byte) (int, error) {
	next := int64(binary.BigEndian.Uint64(b))
	if !assert.True(w.t, next > w.last, fmt.Sprintf("%d not greater than or equal to %d", next, w.last)) {
		return 0, errors.New("Assertion failed")
	}
	w.last = next
	w.numResults++
	return len(b), nil
}
