package index

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
	"io"
)

func TestMergeValueReaders(t *testing.T) {
	reader1 := NewValueSliceReader(1, []Value{
		{Term: 1, DocID: 1},
		{Term: 3, DocID: 1},
		{Term: 1000, DocID: 1},
		{Term: 1001, DocID: 1},
		{Term: 1002, DocID: 1},
	})
	reader2 := NewValueSliceReader(1, []Value{
		{Term: 1, DocID: 2},
		{Term: 2, DocID: 2},
		{Term: 4, DocID: 2},
	})
	reader3 := NewValueSliceReader(2, []Value{
		{Term: 1, DocID: 3},
		{Term: 2, DocID: 4},
		{Term: 50, DocID: 3},
		{Term: 100, DocID: 3},
	})
	reader := MergeValueReaders(reader1, reader2, reader3)
	assert.Equal(t, 4, reader.NumDocs())
	var values []Value
	for {
		block, err := reader.ReadBlock()
		values = append(values, block...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	expectedValues := []Value{
		{Term: 1, DocID: 1},
		{Term: 1, DocID: 2},
		{Term: 1, DocID: 3},
		{Term: 2, DocID: 2},
		{Term: 2, DocID: 4},
		{Term: 3, DocID: 1},
		{Term: 4, DocID: 2},
		{Term: 50, DocID: 3},
		{Term: 100, DocID: 3},
		{Term: 1000, DocID: 1},
		{Term: 1001, DocID: 1},
		{Term: 1002, DocID: 1},
	}
	assert.Equal(t, expectedValues, values)
}