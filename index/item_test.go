package index

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
	"io"
)

func TestMergeItemReaders(t *testing.T) {
	var buf1, buf2, buf3 ItemBuffer
	buf1.Add(1, []uint32{1, 3, 1000, 1001, 1002})
	buf2.Add(2, []uint32{1, 2, 4})
	buf3.Add(3, []uint32{1, 50, 100})
	buf3.Add(4, []uint32{2})
	reader := MergeItemReaders(buf1.Reader(), buf2.Reader(), buf3.Reader())
	assert.Equal(t, 4, reader.NumDocs())
	var items []Item
	for {
		block, err := reader.ReadBlock()
		items = append(items, block...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	expected := []Item{
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
	assert.Equal(t, expected, items)
}