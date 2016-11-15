// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
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

func TestItemBuffer_Delete(t *testing.T) {
	var buf ItemBuffer
	buf.Add(1, []uint32{100, 101})
	buf.Add(3, []uint32{300, 301})
	require.False(t, buf.Delete(2))
	require.Equal(t, 2, buf.NumDocs())
	require.Equal(t, 4, buf.NumItems())
	require.Equal(t, uint32(1), buf.MinDocID())
	require.Equal(t, uint32(3), buf.MaxDocID())
	require.True(t, buf.Delete(3))
	require.Equal(t, 1, buf.NumDocs())
	require.Equal(t, 2, buf.NumItems())
	require.Equal(t, uint32(1), buf.MinDocID())
	require.Equal(t, uint32(1), buf.MaxDocID())
	require.False(t, buf.Delete(3))
	require.Equal(t, 1, buf.NumDocs())
	require.Equal(t, 2, buf.NumItems())
	require.Equal(t, uint32(1), buf.MinDocID())
	require.Equal(t, uint32(1), buf.MaxDocID())
}
