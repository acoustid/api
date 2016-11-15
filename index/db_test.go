// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/acoustid/go-acoustid/index/vfs"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"github.com/stretchr/testify/assert"
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

func TestDB_Transaction_NoCommit(t *testing.T) {
	db, err := Open(vfs.CreateMemDir(), true)
	require.NoError(t, err, "failed to create a new db")
	defer db.Close()

	txn, err := db.Transaction()
	require.NoError(t, err)
	require.NoError(t, txn.Add(1, []uint32{7, 8, 9}), "add failed")
	require.NoError(t, txn.Close(), "close failed")

	hits, err := db.Search([]uint32{9})
	require.NoError(t, err, "search failed")
	require.Empty(t, hits, "hits should be empty because the transaction was not committed")
}

func TestDB_Transaction_DeleteUncommitted(t *testing.T) {
	db, err := Open(vfs.CreateMemDir(), true)
	require.NoError(t, err, "failed to create a new db")
	defer db.Close()

	txn, err := db.Transaction()
	require.NoError(t, err)
	require.NoError(t, txn.Add(1, []uint32{7, 8, 9}), "add failed")
	require.NoError(t, txn.Delete(1), "delete failed")
	require.NoError(t, txn.Commit(), "commit failed")
	require.NoError(t, txn.Close(), "close failed")

	hits, err := db.Search([]uint32{9})
	require.NoError(t, err, "search failed")
	require.Empty(t, hits, "hits should be empty because the only added doc was deleted later")
}

func TestDB_Delete(t *testing.T) {
	fs := vfs.CreateMemDir()
	defer fs.Close()

	func() {
		db, err := Open(fs, true)
		require.NoError(t, err, "failed to create a new db")
		defer db.Close()

		require.NoError(t, db.Add(1, []uint32{7, 8, 9}), "add failed")
		require.NoError(t, db.Delete(1), "delete failed")

		hits, err := db.Search([]uint32{9})
		require.NoError(t, err, "search failed")
		require.Empty(t, hits, "hits should be empty because the only added doc was deleted later")
	}()

	func() {
		db, err := Open(fs, false)
		require.NoError(t, err, "failed to open db")
		defer db.Close()

		hits, err := db.Search([]uint32{9})
		require.NoError(t, err, "search failed")
		require.Empty(t, hits, "hits should be empty because the only added doc was deleted later")
	}()
}

func TestDB_Update(t *testing.T) {
	fs := vfs.CreateMemDir()
	defer fs.Close()

	func() {
		db, err := Open(fs, true)
		require.NoError(t, err, "failed to create a new db")
		defer db.Close()

		require.NoError(t, db.Add(1, []uint32{7, 8, 9}), "add failed")
		require.NoError(t, db.Update(1, []uint32{3, 4, 5}), "update failed")

		hits, err := db.Search([]uint32{9})
		require.NoError(t, err, "search failed")
		require.Empty(t, hits, "hits should be empty, we should not find anything")

		hits, err = db.Search([]uint32{3})
		require.NoError(t, err, "search failed")
		require.Equal(t, map[uint32]int{1: 1}, hits, "we should find the updated doc")
	}()

	func() {
		db, err := Open(fs, false)
		require.NoError(t, err, "failed to open db")
		defer db.Close()

		hits, err := db.Search([]uint32{9})
		require.NoError(t, err, "search failed")
		require.Empty(t, hits, "hits should be empty, we should not find anything")

		hits, err = db.Search([]uint32{3})
		require.NoError(t, err, "search failed")
		require.Equal(t, map[uint32]int{1: 1}, hits, "we should find the updated doc")
	}()
}

func TestDB_DeleteAll(t *testing.T) {
	fs := vfs.CreateMemDir()
	defer fs.Close()

	func() {
		db, err := Open(fs, true)
		require.NoError(t, err, "failed to create a new db")
		defer db.Close()

		require.NoError(t, db.Add(1, []uint32{7, 8, 9}), "add failed")
		require.NoError(t, db.Add(2, []uint32{3, 4, 5}), "add failed")
		require.NoError(t, db.DeleteAll(), "truncate failed")

		hits, err := db.Search([]uint32{7, 8, 9, 3, 4, 5})
		require.NoError(t, err, "search failed")
		require.Empty(t, hits, "hits should be empty, we should not find anything")
	}()

	func() {
		db, err := Open(fs, false)
		require.NoError(t, err, "failed to open db")
		defer db.Close()

		hits, err := db.Search([]uint32{7, 8, 9, 3, 4, 5})
		require.NoError(t, err, "search failed")
		require.Empty(t, hits, "hits should be empty, we should not find anything")
	}()
}

func TestDB_Import(t *testing.T) {
	fs := vfs.CreateMemDir()
	defer fs.Close()

	db, err := Open(fs, true)
	require.NoError(t, err, "failed to create a new db")
	defer db.Close()

	var buf ItemBuffer
	buf.Add(1, []uint32{7, 8, 9})
	buf.Add(2, []uint32{3, 4, 5})
	require.NoError(t, db.Import(buf.Reader()), "import failed")

	hits, err := db.Search([]uint32{7, 8, 9, 3, 4, 5})
	require.NoError(t, err, "search failed")
	require.Equal(t, map[uint32]int{1: 3, 2: 3}, hits, "we should find both imported docs")
}

func TestDB_Reader(t *testing.T) {
	fs := vfs.CreateMemDir()
	defer fs.Close()

	db, err := Open(fs, true)
	require.NoError(t, err, "failed to create a new db")
	defer db.Close()

	require.NoError(t, db.Add(1, []uint32{7, 8, 9}), "add failed")
	require.NoError(t, db.Add(2, []uint32{3, 4, 5}), "add failed")

	items, err := ReadAllItems(db.Reader())
	if assert.NoError(t, err, "failed to read items") {
		expected := []Item{{3, 2}, {4, 2}, {5, 2}, {7, 1}, {8, 1}, {9, 1}}
		assert.Equal(t, expected, items, "read items do not match")
	}
}
