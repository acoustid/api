package index

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	dir := NewMemDir()
	idx, err := Open(dir)
	require.NoError(t, err)
	defer idx.Close()

	idx.Add(1234, []uint32{0xdcfc2563, 0xdcbc2421, 0xddbc3420, 0xdd9c1530, 0xdf9c6d40, 0x4f4ce540, 0x4f0ea5c0})

	err = idx.Search([]uint32{1, 2, 0xdcfc2563, 0xdcbc2421, 0xdeadbeef, 0xffffffff})
	require.NoError(t, err)
}
