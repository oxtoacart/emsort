package emsort

import (
	"encoding/binary"
	"fmt"
	"io"
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
	last := []byte{0}
	next := []byte{0}
	for {
		_, err := file.Read(next)
		if err == io.EOF {
			return
		}
		if !assert.NoError(t, err) {
			return
		}
		if !assert.True(t, next[0] >= last[0], fmt.Sprintf("%d not greater than or equal to %d", next[0], last[0])) {
			return
		}
		last = next
	}
}

type testData struct {
}

func (td *testData) Fill(fn func(interface{}) error) error {
	for i := 0; i < 10000; i++ {
		err := fn(rand.Int63())
		if err != nil {
			return err
		}
	}
	return nil
}

func (td *testData) Size(v interface{}) int {
	return 8
}

func (td *testData) Read(r io.Reader) (interface{}, error) {
	b := make([]byte, 8)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

func (td *testData) Write(w io.Writer, v interface{}) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v.(int64)))
	_, err := w.Write(b)
	return err
}

func (td *testData) Less(a, b interface{}) bool {
	return a.(int64) < b.(int64)
}
