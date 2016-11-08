package index

import (
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/stretchr/testify/require"
	"testing"
	"math/rand"
)

func TestDB(t *testing.T) {
	fs := vfs.CreateMemDir()

	db, err := Open(fs, true)
	require.NoError(t, err)
	defer db.Close()

	err = db.Add(1234, []uint32{0xdcfc2563, 0xdcbc2421, 0xddbc3420, 0xdd9c1530, 0xdf9c6d40, 0x4f4ce540, 0x4f0ea5c0})
	require.NoError(t, err)

	err = db.Add(5678, []uint32{123, 53})
	require.NoError(t, err)

	r := rand.New(rand.NewSource(0))
	for i := 0; i < 10; i++ {
		var terms [1000]uint32
		for j := range terms {
			terms[j] = r.Uint32()
		}
		err = db.Add(r.Uint32(), terms[:])
		require.NoError(t, err)
	}

	hits, err := db.Search([]uint32{1, 2, 0xdcfc2563, 0xdcbc2421, 0xdeadbeef, 0xffffffff})
	require.NoError(t, err)
	require.Equal(t, hits, map[uint32]int{1234: 2})

	err = db.Delete(1234)
	require.NoError(t, err)

	db2, err := Open(fs, false)
	require.NoError(t, err)
	defer db2.Close()
}
