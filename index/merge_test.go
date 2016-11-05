package index

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestTieredMergePolicy_FindMerges_MergeEqual(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 0
	mp.MaxMergeAtOnce = 2
	mp.MaxSegmentsPerTier = 1
	segments := []*Segment{
		{ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		{ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		{ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}
	merges := mp.FindMerges(segments, 0)
	require.Equal(t, 1, len(merges))
	require.Equal(t, 2, len(merges[0].Segments))
	require.Contains(t, merges[0].Segments, segments[1])
	require.Contains(t, merges[0].Segments, segments[2])
}

func TestTieredMergePolicy_FindMerges_NoMerges(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 0
	mp.MaxMergeAtOnce = 2
	mp.MaxSegmentsPerTier = 1
	segments := []*Segment{
		{ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 2}},
		{ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		{ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}
	merges := mp.FindMerges(segments, 0)
	require.Equal(t, 0, len(merges))
}

func TestTieredMergePolicy_FindMerges_PreferSmaller(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 0
	mp.MaxMergeAtOnce = 2
	mp.MaxSegmentsPerTier = 1
	segments := []*Segment{
		{ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 4}},
		{ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 3}},
		{ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 2}},
		{ID: 3, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		{ID: 4, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}
	merges := mp.FindMerges(segments, 0)
	require.Equal(t, 1, len(merges))
	require.Equal(t, 2, len(merges[0].Segments))
	require.Contains(t, merges[0].Segments, segments[3])
	require.Contains(t, merges[0].Segments, segments[4])
}

func TestTieredMergePolicy_FindMerges_IgnoreTooLarge(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 0
	mp.MaxMergeAtOnce = 2
	mp.MaxSegmentsPerTier = 1
	segments := []*Segment{
		{ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: mp.MaxMergedSegmentSize}},
		{ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 3}},
		{ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 2}},
		{ID: 3, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		{ID: 4, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}
	merges := mp.FindMerges(segments, 0)
	require.Equal(t, 1, len(merges))
	require.Equal(t, 2, len(merges[0].Segments))
	require.Contains(t, merges[0].Segments, segments[3])
	require.Contains(t, merges[0].Segments, segments[4])
}

func TestTieredMergePolicy_FindMerges_Floored(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 10
	mp.MaxMergeAtOnce = 4
	mp.MaxSegmentsPerTier = 1
	segments := []*Segment{
		{ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 4}},
		{ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 3}},
		{ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 2}},
		{ID: 3, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		{ID: 4, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}
	merges := mp.FindMerges(segments, 0)
	require.Equal(t, 1, len(merges))
	require.Equal(t, 4, len(merges[0].Segments))
	require.Contains(t, merges[0].Segments, segments[1])
	require.Contains(t, merges[0].Segments, segments[2])
	require.Contains(t, merges[0].Segments, segments[3])
	require.Contains(t, merges[0].Segments, segments[4])
}
