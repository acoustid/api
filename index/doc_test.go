package index

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestSingleDocIterator_Read(t *testing.T) {
	it := SingleDocIterator(1234, []uint32{6, 5, 4, 3, 2, 1})
	buf := make([]TermDocID, 10)

	n, err := it.Read(buf[:1])
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []TermDocID{PackTermDocID(1, 1234)}, buf[:n])

	n, err = it.Read(buf[:2])
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []TermDocID{PackTermDocID(2, 1234), PackTermDocID(3, 1234)}, buf[:n])

	n, err = it.Read(buf)
	require.Equal(t, err, EOF)
	require.Equal(t, 3, n)
	require.Equal(t, []TermDocID{PackTermDocID(4, 1234), PackTermDocID(5, 1234), PackTermDocID(6, 1234)}, buf[:n])

	n, err = it.Read(buf)
	require.Equal(t, err, EOF)
	require.Equal(t, 0, n)

	n, err = it.Read(buf)
	require.Equal(t, err, EOF)
	require.Equal(t, 0, n)
}

func TestSingleDocIterator_SeekTo(t *testing.T) {
	it := SingleDocIterator(1234, []uint32{6, 5, 4, 3, 2, 1})
	buf := make([]TermDocID, 10)

	found, err := it.SeekTo(4)
	require.NoError(t, err)
	require.True(t, found)

	n, err := it.Read(buf)
	require.Equal(t, err, EOF)
	require.Equal(t, 3, n)
	require.Equal(t, []TermDocID{PackTermDocID(4, 1234), PackTermDocID(5, 1234), PackTermDocID(6, 1234)}, buf[:n])
}

func TestSingleDocIterator_SeekTo_NotFound(t *testing.T) {
	it := SingleDocIterator(1234, []uint32{6, 5, 4, 3, 2, 1})
	buf := make([]TermDocID, 10)

	found, err := it.SeekTo(10)
	require.NoError(t, err)
	require.False(t, found)

	n, err := it.Read(buf)
	require.Equal(t, err, EOF)
	require.Equal(t, 0, n)
}
