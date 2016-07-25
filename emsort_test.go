package emsort

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundTrip(t *testing.T) {
	d := &testData{}
	filename, err := Sorted(d, 1000)
	defer os.Remove(filename)
	if !assert.NoError(t, err) {
		return
	}
	file, err := os.Open(filename)
	if !assert.NoError(t, err) {
		return
	}
	defer file.Close()
	last := int64(0)
	next := int64(0)
	b := make([]byte, 8)
	for {
		_, err := io.ReadFull(file, b)
		if err == io.EOF {
			return
		}
		next = int64(binary.BigEndian.Uint64(b))
		if !assert.NoError(t, err) {
			return
		}
		if !assert.True(t, next > last, fmt.Sprintf("%d not greater than or equal to %d", next, last)) {
			return
		}
		last = next
	}
}

type testData struct {
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
