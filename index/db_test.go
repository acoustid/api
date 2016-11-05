package index

import (
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIndex(t *testing.T) {
	fs := vfs.CreateMemDir()

	db, err := Open(fs, true)
	require.NoError(t, err)
	defer db.Close()

	db.Add(1234, []uint32{0xdcfc2563, 0xdcbc2421, 0xddbc3420, 0xdd9c1530, 0xdf9c6d40, 0x4f4ce540, 0x4f0ea5c0})
	db.Add(5678, []uint32{123, 53})

	hits, err := db.Search([]uint32{1, 2, 0xdcfc2563, 0xdcbc2421, 0xdeadbeef, 0xffffffff})
	require.NoError(t, err)
	assert.Equal(t, hits, map[uint32]int{1234: 2})

	db2, err := Open(fs, false)
	require.NoError(t, err)
	defer db2.Close()

}