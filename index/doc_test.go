package index

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSingleDocIterator_Read(t *testing.T) {
	it := NewSingleDocReader(1234, []uint32{6, 5, 4, 3, 2, 1})
	buf := make([]Value, 10)

	n, err := it.ReadValues(buf[:1])
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []Value{{1, 1234}}, buf[:n])

	n, err = it.ReadValues(buf[:2])
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []Value{{2, 1234}, {3, 1234}}, buf[:n])

	n, err = it.ReadValues(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, []Value{{4, 1234}, {5, 1234}, {6, 1234}}, buf[:n])

	n, err = it.ReadValues(buf)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	n, err = it.ReadValues(buf)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}
