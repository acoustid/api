// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"testing"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/stretchr/testify/require"
)

func TestManifest_Rebase(t *testing.T) {
	fs := vfs.CreateMemDir()

	newSegment := func(id uint32, docID uint32, terms []uint32) *Segment {
		var buf ItemBuffer
		buf.Add(docID, terms)
		s, err := CreateSegment(fs, id, buf.Reader())
		require.NoError(t, err)
		return s
	}

	t.Run("Noop", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		m1.ID = 1

		m2 := m1.Clone()
		m2.AddSegment(newSegment(2, 2, []uint32{2}))
		err := m2.Rebase(m1)
		require.NoError(t, err)
		m2.UpdateStats()
		require.Equal(t, 2, m2.NumDocs)
		require.Contains(t, m2.Segments, uint32(1))
		require.Contains(t, m2.Segments, uint32(2))
	})

	t.Run("Add", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		m1.ID = 1

		m2 := m1.Clone()
		m2.AddSegment(newSegment(2, 2, []uint32{2}))
		m2.ID = 2

		m3 := m1.Clone()
		m3.AddSegment(newSegment(3, 3, []uint32{3}))
		err := m3.Rebase(m2)
		require.NoError(t, err)
		m3.UpdateStats()
		require.Equal(t, 3, m3.NumDocs)
		require.Contains(t, m3.Segments, uint32(1))
		require.Contains(t, m3.Segments, uint32(2))
		require.Contains(t, m3.Segments, uint32(3))
	})

	t.Run("Delete", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		m1.ID = 1

		m2 := m1.Clone()
		m2.Segments[1].Delete(1)
		m2.Segments[1].SaveUpdate(fs, 2)
		m2.ID = 2

		m3 := m1.Clone()
		m3.AddSegment(newSegment(3, 3, []uint32{3}))
		err := m3.Rebase(m2)
		require.NoError(t, err)
		m3.UpdateStats()
		require.Equal(t, 2, m3.NumDocs)
		require.Equal(t, 1, m3.NumDeletedDocs)
		require.Contains(t, m3.Segments, uint32(1))
		require.Contains(t, m3.Segments, uint32(3))
	})

	t.Run("DeleteMerge1", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		m1.AddSegment(newSegment(2, 2, []uint32{2}))
		m1.ID = 1

		m2 := m1.Clone()
		m2.Segments[1].Delete(1)
		m2.Segments[1].SaveUpdate(fs, 2)
		m2.ID = 2

		m3 := m1.Clone()
		m3.Segments[2].Delete(2)
		err := m3.Rebase(m2)
		require.NoError(t, err)
		m3.UpdateStats()
		require.Equal(t, 2, m3.NumDocs)
		require.Equal(t, 2, m3.NumDeletedDocs)
		require.Contains(t, m3.Segments, uint32(1))
		require.Contains(t, m3.Segments, uint32(2))
	})

	t.Run("DeleteMerge2", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		m1.ID = 1

		m2 := m1.Clone()
		m2.Segments[1].Delete(1)
		m2.Segments[1].SaveUpdate(fs, 2)
		m2.ID = 2

		m3 := m1.Clone()
		m3.Segments[1].Delete(1)
		err := m3.Rebase(m2)
		require.NoError(t, err)
		m3.UpdateStats()
		require.Equal(t, 1, m3.NumDocs)
		require.Equal(t, 1, m3.NumDeletedDocs)
		require.Contains(t, m3.Segments, uint32(1))
	})

}