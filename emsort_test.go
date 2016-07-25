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

func (td *testData) Fill(fn func([]byte) error) error {
	for i := 0; i < 10000; i++ {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(rand.Int63()))
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

func (td *testData) Write(w io.Writer, b []byte) error {
	_, err := w.Write(b)
	return err
}

func (td *testData) Less(a, b []byte) bool {
	return binary.LittleEndian.Uint64(a) < binary.LittleEndian.Uint64(b)
}
