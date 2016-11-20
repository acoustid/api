// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"bytes"
	"fmt"
	"go4.org/sort"
	"log"
	"math"
)

// Merge provides information necessary to perform a merge operation, resulting in one new segment.
type Merge struct {
	Segments []*Segment
	Score    float64
	Size     int
}

func (m Merge) String() string {
	var buf bytes.Buffer
	buf.WriteString("{Segments: [")
	for i, s := range m.Segments {
		if i == 0 {
			buf.WriteString(fmt.Sprintf("%v", s.ID))
		} else {
			buf.WriteString(fmt.Sprintf(" %v", s.ID))
		}
	}
	buf.WriteString(fmt.Sprintf("], Score: %v", m.Size))
	buf.WriteString(fmt.Sprintf(", Size: %v", m.Size))
	buf.WriteString("}")
	return buf.String()
}

// MergePolicy determines a sequence of merge operations.
type MergePolicy interface {
	FindMerges(segments []*Segment, maxSize int) (merges []Merge)
}

// TieredMergePolicy is an adaptation of the algorithm from Lucene's TieredMergePolicy written by Michael McCandless.
//
// https://github.com/apache/lucene-solr/blob/master/lucene/core/src/java/org/apache/lucene/index/TieredMergePolicy.java
type TieredMergePolicy struct {
	// FloorSegmentSize is the smallest segment size we will consider.  Segments smaller than this
	// are "rounded up" to this size, ie treated as equal (floor) size for merge selection.
	// This is to prevent frequent flushing of tiny segments from allowing a long tail in the index.
	// Default is 1 MB.
	FloorSegmentSize int

	// MaxMergedSegmentSize is the maximum size of a segment produced during normal merging.
	// This setting is approximate: the estimate of the merged segment size is made by summing
	// sizes of to-be-merged segments (compensating for percent deleted docs).  Default is 2 GB.
	MaxMergedSegmentSize int

	// MaxMergeAtOnce is the maximum number of segments to be merged at a time during normal merging.
	// Default is 10.
	MaxMergeAtOnce int

	// MaxSegmentsPerTier is the allowed number of segments per tier.  Smaller values mean more merging
	// but fewer segments.  This should be >= MaxMergeAtOnce otherwise you'll force too much merging to occur.
	// Default is 10.
	MaxSegmentsPerTier int

	Verbose bool
}

func NewTieredMergePolicy() *TieredMergePolicy {
	return &TieredMergePolicy{
		FloorSegmentSize:     1024 * 1024,
		MaxMergedSegmentSize: 1024 * 1024 * 1024 * 2,
		MaxMergeAtOnce:       10,
		MaxSegmentsPerTier:   10,
	}
}

func (mp *TieredMergePolicy) floorSize(size int) int {
	if size < mp.FloorSegmentSize {
		return mp.FloorSegmentSize
	}
	return size
}

func (mp *TieredMergePolicy) findBestMerge(segments []*Segment, maxSize int) (bestMerge *Merge) {
	for i := 0; i <= len(segments)-mp.MaxMergeAtOnce; i++ {
		var merge Merge
		var mergeSize, mergeSizeFloored int
		var hitTooLarge bool
		for j := i; j < len(segments); j++ {
			segment := segments[j]
			if segment.Size()+mergeSize > maxSize {
				hitTooLarge = true
				continue
			}
			mergeSize += segment.Size()
			mergeSizeFloored += mp.floorSize(segment.Size())
			merge.Segments = append(merge.Segments, segment)
			if len(merge.Segments) >= mp.MaxMergeAtOnce {
				break
			}
		}

		var skew float64
		if hitTooLarge {
			skew = 1.0 / float64(mp.MaxMergeAtOnce)
		} else {
			skew = float64(mp.floorSize(merge.Segments[0].Size())) / float64(mergeSizeFloored)
		}
		merge.Score = skew * math.Pow(float64(mergeSize), 0.05)
		merge.Size = mergeSize

		if mp.Verbose {
			log.Printf("FindMerges: merge %s", merge)
		}

		if bestMerge == nil || merge.Score < bestMerge.Score {
			bestMerge = &merge
		}
	}
	return
}

func (mp *TieredMergePolicy) FindBestMerge(origSegments map[uint32]*Segment, maxSize int) *Merge {
	merges := mp.FindMerges(origSegments, maxSize)
	if len(merges) == 0 {
		return nil
	}
	return merges[0]
}

func (mp *TieredMergePolicy) FindMerges(origSegments map[uint32]*Segment, maxSize int) (merges []*Merge) {
	if maxSize == 0 {
		maxSize = mp.MaxMergedSegmentSize
	}

	// Filter our segments that are over-sized and we could not potentially merge them.
	segments := make([]*Segment, 0, len(origSegments))
	for _, segment := range origSegments {
		if mp.Verbose {
			size := segment.Size()
			var extra string
			if size > maxSize/2 {
				extra += " [skip: too large]"
			} else if size < mp.FloorSegmentSize {
				extra += " [floored]"
			}
			log.Printf("FindMerges: Segment: %d, Size: %d%s", segment.ID, segment.Size(), extra)
		}
		if segment.Size() <= maxSize/2 {
			segments = append(segments, segment)
		}
	}

	// Sort segments by their size in decreasing order.
	sort.Slice(segments, func(i, j int) bool { return segments[i].Size() >= segments[j].Size() })

	// Compute the max allowed segments in the index considering the merge policy options.
	var allowedSegmentCount, remainingSize int
	for _, segment := range segments {
		remainingSize += segment.Size()
	}
	levelSize := mp.floorSize(segments[len(segments)-1].Size())
	for {
		levelSegmentCount := (remainingSize + levelSize - 1) / levelSize
		if levelSegmentCount < mp.MaxSegmentsPerTier {
			allowedSegmentCount += levelSegmentCount
			break
		}
		allowedSegmentCount += mp.MaxSegmentsPerTier
		remainingSize -= mp.MaxSegmentsPerTier * levelSize
		levelSize *= mp.MaxMergeAtOnce
	}

	// Find possible merges until we run out of candidates.
	for len(segments) > allowedSegmentCount {
		merge := mp.findBestMerge(segments, maxSize)
		if merge == nil {
			break
		}
		merges = append(merges, merge)

		if mp.Verbose {
			log.Printf("FindMerges: merge %s", merge)
		}

		// Filter out segments that we just selected to be merged.
		removed := make(map[uint32]bool, len(merge.Segments))
		for _, segment := range merge.Segments {
			removed[segment.ID] = true
		}
		eligible := segments[:0]
		for _, segment := range segments {
			if !removed[segment.ID] {
				eligible = append(eligible, segment)
			}
		}
		segments = eligible
	}

	return
}
