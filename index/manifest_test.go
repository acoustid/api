// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"testing"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/stretchr/testify/require"
)

func newTestSegment(t *testing.T, fs vfs.FileSystem, id uint32, docID uint32, terms []uint32) *Segment {
	var buf ItemBuffer
	buf.Add(docID, terms)
	s, err := CreateSegment(fs, id, buf.Reader())
	require.NoError(t, err)
	return s
}

func newTestSegment2(t *testing.T, fs vfs.FileSystem, id uint32, docID uint32, terms []uint32, docID2 uint32, terms2 []uint32) *Segment {
	var buf ItemBuffer
	buf.Add(docID, terms)
	buf.Add(docID2, terms2)
	s, err := CreateSegment(fs, id, buf.Reader())
	require.NoError(t, err)
	return s
}

func TestManifest_Rebase(t *testing.T) {
	fs := vfs.CreateMemDir()

	newSegment := func(id uint32, docID uint32, terms []uint32) *Segment {
		var buf ItemBuffer
		buf.Add(docID, terms)
		s, err := CreateSegment(fs, id, buf.Reader())
		require.NoError(t, err)
		return s
	}

	t.Run("Linear", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		require.NoError(t, m1.Commit(fs, 1, nil))

		m2 := m1.Clone()
		m2.AddSegment(newSegment(2, 2, []uint32{2}))
		require.NoError(t, m2.Commit(fs, 2, m1))

		require.Equal(t, 2, m2.NumDocs)
		require.Equal(t, 0, m2.NumDeletedDocs)
		require.Equal(t, 2, len(m2.Segments))
		require.Contains(t, m2.Segments, uint32(1))
		require.Contains(t, m2.Segments, uint32(2))
	})

	t.Run("MergeAdds", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		require.NoError(t, m1.Commit(fs, 1, nil))

		m2 := m1.Clone()
		m2.AddSegment(newSegment(2, 2, []uint32{2}))
		require.NoError(t, m2.Commit(fs, 2, m1))

		m3 := m1.Clone()
		m3.AddSegment(newSegment(3, 3, []uint32{3}))
		require.NoError(t, m3.Commit(fs, 3, m2))

		require.Equal(t, 3, m3.NumDocs)
		require.Equal(t, 0, m3.NumDeletedDocs)
		require.Equal(t, 3, len(m3.Segments))
		require.Contains(t, m3.Segments, uint32(1))
		require.Contains(t, m3.Segments, uint32(2))
		require.Contains(t, m3.Segments, uint32(3))
	})

	t.Run("MergeDuplicateAdds", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		require.NoError(t, m1.Commit(fs, 1, nil))

		m2 := m1.Clone()
		m2.AddSegment(newSegment(2, 2, []uint32{2}))
		require.NoError(t, m2.Commit(fs, 2, m1))

		m3 := m1.Clone()
		m3.AddSegment(newSegment(3, 2, []uint32{3}))
		require.NoError(t, m3.Commit(fs, 3, m2))

		require.Equal(t, 3, m3.NumDocs)
		require.Equal(t, 1, m3.NumDeletedDocs)
		require.Equal(t, 3, len(m3.Segments))
		require.Contains(t, m3.Segments, uint32(1))
		require.Contains(t, m3.Segments, uint32(2))
		require.Contains(t, m3.Segments, uint32(3))
	})

	t.Run("MergeDeleteWithAdd", func(t *testing.T) {
		m1 := &Manifest{}
		m1.Reset()
		m1.AddSegment(newSegment(1, 1, []uint32{1}))
		require.NoError(t, m1.Commit(fs, 1, nil))

		m2 := m1.Clone()
		m2.Segments[1].Delete(1)
		require.NoError(t, m2.Commit(fs, 2, m1))

		m3 := m1.Clone()
		m3.AddSegment(newSegment(3, 3, []uint32{3}))
		require.NoError(t, m3.Commit(fs, 2, m2))

		require.Equal(t, 2, m3.NumDocs)
		require.Equal(t, 1, m3.NumDeletedDocs)
		require.Equal(t, 2, len(m3.Segments))
		require.Contains(t, m3.Segments, uint32(1))
		require.Contains(t, m3.Segments, uint32(3))
	})

}

// TestManifest_Commit_RemoveSegmentConflict tests that we can detect a conflict caused by concurrent removal of a segment.
func TestManifest_Commit_RemoveSegmentConflict(t *testing.T) {
	fs := vfs.CreateMemDir()

	m := NewManifest()
	m.AddSegment(newTestSegment(t, fs, 1, 1, []uint32{1}))
	m.AddSegment(newTestSegment(t, fs, 2, 2, []uint32{1}))
	require.NoError(t, m.Commit(fs, 2, nil))

	m2 := m.Clone()
	m2.RemoveSegment(m2.Segments[1])
	require.NoError(t, m2.Commit(fs, 3, m))

	m3 := m.Clone()
	m3.RemoveSegment(m3.Segments[1])
	err := m3.Commit(fs, 4, m2)
	require.Error(t, err, "commit should fail, we removed the same segment twice")
	require.True(t, IsConflict(err), "commit should fail with a conflict error")
}

// TestManifest_Commit_MergeDeletedDocs tests that we can merge concurrent deletes of two different docs in two different segments.
func TestManifest_Commit_MergeDeletedDocs(t *testing.T) {
	fs := vfs.CreateMemDir()

	m := NewManifest()
	m.AddSegment(newTestSegment(t, fs, 1, 1, []uint32{1}))
	m.AddSegment(newTestSegment(t, fs, 2, 2, []uint32{2}))
	require.NoError(t, m.Commit(fs, 100, nil))

	m2 := m.Clone()
	m2.Segments[1].Delete(1)
	require.NoError(t, m2.Commit(fs, 200, m))

	m3 := m.Clone()
	m3.Segments[2].Delete(2)
	require.NoError(t, m3.Commit(fs, 300, m2), "commit should not fail")

	require.Equal(t, 2, m3.NumDocs)
	require.Equal(t, 2, m3.NumDeletedDocs)
	require.Equal(t, 2, len(m3.Segments))
	require.Contains(t, m3.Segments, uint32(1))
	require.Contains(t, m3.Segments, uint32(2))
	require.True(t, m3.Segments[1].deletedDocs.Contains(1))
	require.True(t, m3.Segments[2].deletedDocs.Contains(2))
}

// TestManifest_Commit_MergeDeletedDocsSameSegment tests that we can merge concurrent deletes of two different docs in the same segment.
func TestManifest_Commit_MergeDeletedDocsSameSegment(t *testing.T) {
	fs := vfs.CreateMemDir()

	m := NewManifest()
	m.AddSegment(newTestSegment2(t, fs, 1, 1, []uint32{1}, 2, []uint32{2}))
	require.NoError(t, m.Commit(fs, 1, nil))

	m2 := m.Clone()
	m2.Segments[1].Delete(1)
	require.NoError(t, m2.Commit(fs, 2, m))

	m3 := m.Clone()
	m3.Segments[1].Delete(2)
	require.NoError(t, m3.Commit(fs, 3, m2), "commit should not fail")

	require.Equal(t, 2, m3.NumDocs)
	require.Equal(t, 2, m3.NumDeletedDocs)
	require.Equal(t, 1, len(m3.Segments))
	require.Contains(t, m3.Segments, uint32(1))
	require.True(t, m3.Segments[1].deletedDocs.Contains(1))
}

// TestManifest_Commit_MergeDeletedDocsSameDoc tests that we can merge concurrent deletes of the same different doc in the same segment.
func TestManifest_Commit_MergeDeletedDocsSameDoc(t *testing.T) {
	fs := vfs.CreateMemDir()

	m := NewManifest()
	m.AddSegment(newTestSegment(t, fs, 1, 1, []uint32{1}))
	require.NoError(t, m.Commit(fs, 1, nil))

	m2 := m.Clone()
	m2.Segments[1].Delete(1)
	require.NoError(t, m2.Commit(fs, 2, m))

	m3 := m.Clone()
	m3.Segments[1].Delete(1)
	require.NoError(t, m3.Commit(fs, 3, m2), "commit failed")

	require.Equal(t, 1, m3.NumDocs)
	require.Equal(t, 1, m3.NumDeletedDocs)
	require.Equal(t, 1, len(m3.Segments))
	require.Contains(t, m3.Segments, uint32(1))
	require.True(t, m3.Segments[1].deletedDocs.Contains(1))
}

func TestManifest_Commit_MergeAddedDocs(t *testing.T) {
	fs := vfs.CreateMemDir()

	m := NewManifest()
	require.NoError(t, m.Commit(fs, 100, nil))

	m2 := m.Clone()
	m2.AddSegment(newTestSegment(t, fs, 200, 1, []uint32{1}))
	require.NoError(t, m2.Commit(fs, 200, m))

	m3 := m.Clone()
	m3.AddSegment(newTestSegment(t, fs, 300, 2, []uint32{2}))
	require.NoError(t, m3.Commit(fs, 300, m2))

	require.Equal(t, 2, m3.NumDocs)
	require.Equal(t, 0, m3.NumDeletedDocs)
	require.Len(t, m3.Segments, 2)
	require.Contains(t, m3.Segments, uint32(200))
	require.Contains(t, m3.Segments, uint32(300))
	require.True(t, m3.Segments[200].Contains(1))
	require.True(t, m3.Segments[300].Contains(2))
}

func TestManifest_Commit_ResolveDuplicateDocs(t *testing.T) {
	fs := vfs.CreateMemDir()

	m := NewManifest()
	require.NoError(t, m.Commit(fs, 100, nil))

	m2 := m.Clone()
	m2.AddSegment(newTestSegment(t, fs, 200, 1, []uint32{1}))
	require.NoError(t, m2.Commit(fs, 200, m))

	m3 := m.Clone()
	m3.AddSegment(newTestSegment(t, fs, 300, 1, []uint32{2}))
	require.NoError(t, m3.Commit(fs, 300, m2))

	require.Equal(t, 2, m3.NumDocs)
	require.Equal(t, 1, m3.NumDeletedDocs)
	require.Len(t, m3.Segments, 2)
	require.Contains(t, m3.Segments, uint32(200))
	require.Contains(t, m3.Segments, uint32(300))
	require.False(t, m3.Segments[200].Contains(1))
	require.True(t, m3.Segments[300].Contains(1))
}

func TestManifest_Commit_ResolveConcurrentUpdate1(t *testing.T) {
	fs := vfs.CreateMemDir()

	m := NewManifest()
	require.NoError(t, m.Commit(fs, 1, nil))

	m2 := m.Clone()
	m2.AddSegment(newTestSegment(t, fs, 2, 1, []uint32{2}))
	require.NoError(t, m2.Commit(fs, 2, m))

	m3 := m2.Clone()
	m3.Segments[2].Delete(1)
	require.NoError(t, m3.Commit(fs, 3, m))

	m4 := m.Clone()
	m4.AddSegment(newTestSegment(t, fs, 4, 1, []uint32{4}))
	require.NoError(t, m4.Commit(fs, 4, m3))

	require.Equal(t, 2, m4.NumDocs)
	require.Equal(t, 1, m4.NumDeletedDocs)
	require.Len(t, m4.Segments, 2)
	require.Contains(t, m4.Segments, uint32(2))
	require.Contains(t, m4.Segments, uint32(4))
	require.False(t, m4.Segments[2].Contains(1))
	require.True(t, m4.Segments[4].Contains(1))
}

func TestManifest_Commit_ReapplyDelete(t *testing.T) {
	fs := vfs.CreateMemDir()

	m := NewManifest()
	m.AddSegment(newTestSegment(t, fs, 1, 1, []uint32{1}))
	require.NoError(t, m.Commit(fs, 1, nil))

	m2 := m.Clone()
	m2.AddSegment(newTestSegment(t, fs, 2, 1, []uint32{2}))
	require.NoError(t, m2.Commit(fs, 2, m))

	m3 := m.Clone()
	m3.Delete(1)
	require.NoError(t, m3.Commit(fs, 3, m2))

	require.Equal(t, 2, m3.NumDocs)
	require.Equal(t, 2, m3.NumDeletedDocs)
	require.Len(t, m3.Segments, 2)
	require.Contains(t, m3.Segments, uint32(1))
	require.Contains(t, m3.Segments, uint32(2))
	require.False(t, m3.Segments[1].Contains(1))
	require.False(t, m3.Segments[2].Contains(1))
}
