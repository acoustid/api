// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTieredMergePolicy_FindMerges_MergeEqual(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 0
	mp.MaxMergeAtOnce = 3
	mp.MaxSegmentsPerTier = 1
	manifest := NewManifest()

	manifest.Segments = map[uint32]*Segment{
		0: {ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		1: {ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		2: {ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}

	merge := mp.FindBestMerge(manifest, 0)
	require.Equal(t, 3, len(merge.Segments))
	require.Contains(t, merge.Segments, manifest.Segments[0])
	require.Contains(t, merge.Segments, manifest.Segments[1])
	require.Contains(t, merge.Segments, manifest.Segments[2])
}

func TestTieredMergePolicy_FindMerges_NoMerges(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 0
	mp.MaxMergeAtOnce = 2
	mp.MaxSegmentsPerTier = 1

	manifest := NewManifest()
	manifest.Segments = map[uint32]*Segment{
		0: {ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 2}},
		1: {ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		2: {ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}

	merge := mp.FindBestMerge(manifest, 0)
	require.Nil(t, merge)
}

func TestTieredMergePolicy_FindMerges_PreferSmaller(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 0
	mp.MaxMergeAtOnce = 2
	mp.MaxSegmentsPerTier = 1

	manifest := NewManifest()
	manifest.Segments = map[uint32]*Segment{
		0: {ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 4}},
		1: {ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 3}},
		2: {ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 2}},
		3: {ID: 3, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		4: {ID: 4, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}

	merge := mp.FindBestMerge(manifest, 0)
	require.Equal(t, 2, len(merge.Segments))
	require.Contains(t, merge.Segments, manifest.Segments[3])
	require.Contains(t, merge.Segments, manifest.Segments[4])
}

func TestTieredMergePolicy_FindMerges_IgnoreTooLarge(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 0
	mp.MaxMergeAtOnce = 2
	mp.MaxSegmentsPerTier = 1

	manifest := NewManifest()
	manifest.Segments = map[uint32]*Segment{
		0: {ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: mp.MaxMergedSegmentSize}},
		1: {ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 3}},
		2: {ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 2}},
		3: {ID: 3, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		4: {ID: 4, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}

	merge := mp.FindBestMerge(manifest, 0)
	require.Equal(t, 2, len(merge.Segments))
	require.Contains(t, merge.Segments, manifest.Segments[3])
	require.Contains(t, merge.Segments, manifest.Segments[4])
}

func TestTieredMergePolicy_FindMerges_Floored(t *testing.T) {
	mp := NewTieredMergePolicy()
	mp.FloorSegmentSize = 10
	mp.MaxMergeAtOnce = 4
	mp.MaxSegmentsPerTier = 1

	manifest := NewManifest()
	manifest.Segments = map[uint32]*Segment{
		0: {ID: 0, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 4}},
		1: {ID: 1, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 3}},
		2: {ID: 2, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 2}},
		3: {ID: 3, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
		4: {ID: 4, Meta: SegmentMeta{BlockSize: 1, NumBlocks: 1}},
	}

	merge := mp.FindBestMerge(manifest, 0)
	require.Equal(t, 4, len(merge.Segments))
	require.Contains(t, merge.Segments, manifest.Segments[1])
	require.Contains(t, merge.Segments, manifest.Segments[2])
	require.Contains(t, merge.Segments, manifest.Segments[3])
	require.Contains(t, merge.Segments, manifest.Segments[4])
}
