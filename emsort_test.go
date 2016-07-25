package emsort

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundTrip(t *testing.T) {
	td := &testData{}
	err := Sorted(td, 1000)
	if assert.NoError(t, err) {
		assert.Equal(t, 10000, td.numResults)
	}
}

type testData struct {
	last       int64
	numResults int
	t          *testing.T
}

func (td *testData) Fill(fn func([]byte) error) error {
	halfMaxInt := int64(math.MaxInt64 / 2)
	for i := 0; i < 10000; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(halfMaxInt+(rand.Int63n(halfMaxInt))))
		err := fn(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (td *testData) Read(r io.Reader) ([]byte, error) {
	b := make([]byte, 8)
	_, err := io.ReadFull(r, b)
	return b, err
}

func (td *testData) OnSorted(b []byte) error {
	next := int64(binary.BigEndian.Uint64(b))
	assert.True(td.t, next > td.last, fmt.Sprintf("%d not greater than or equal to %d", next, td.last))
	td.last = next
	td.numResults++
	return nil
}
