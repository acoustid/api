package index

import (
	"go4.org/sort"
	"math"
	"log"
	"fmt"
	"strings"
)

// Merge provides information necessary to perform a merge operation, resulting in one new segment.
type Merge struct {
	Segments []*Segment
	Score    float64
	Size     int
}

// MergePolicy determines a sequence of merge operations.
type MergePolicy interface {
	FindMerges(segments []*Segment) (merges []Merge)
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

		if mp.Verbose {
			var ms []string
			for _, s := range merge.Segments {
				ms = append(ms, fmt.Sprintf("%s", s.ID))
			}
			log.Printf("FindMerges: maybe merge segments=%v size=%v score=%v skew=%v", strings.Join(ms, ","), merge.Size, merge.Score, skew)
		}

		if bestMerge == nil || merge.Score < bestMerge.Score {
			bestMerge = &merge
		}
	}
	return
}

func (mp *TieredMergePolicy) FindMerges(origSegments []*Segment, maxSize int) (merges []*Merge) {
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
			log.Printf("FindMerges: seg=%s size=%d%s", segment.ID, segment.Size(), extra)
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

	count := len(segments)

	if mp.Verbose {
		log.Printf("FindMerges: count=%v allowed=%v eligible=%v", count, allowedSegmentCount, len(segments))
	}

	// Find possible merges until we run out of candidates.
	for len(segments) > allowedSegmentCount {
		merge := mp.findBestMerge(segments, maxSize)
		if merge == nil {
			break
		}
		if mp.Verbose {
			var ms []string
			for _, s := range merge.Segments {
				ms = append(ms, fmt.Sprintf("%s", s.ID))
			}
			log.Printf("FindMerges: merge segments=%v size=%v score=%v", strings.Join(ms, ","), merge.Size, merge.Score)
		}
		merges = append(merges, merge)

		// Remove the merged segments from the list of candidates to be merged next.
		remove := make(map[SegmentID]bool, len(merge.Segments))
		for _, segment := range merge.Segments {
			remove[segment.ID] = true
		}
		i := 0
		for _, segment := range segments {
			if !remove[segment.ID] {
				segments[i] = segment
				i++
			}
		}
		segments = segments[:i]

		if mp.Verbose {
			log.Printf("FindMerges: count=%v allowed=%v eligible=%v", count, allowedSegmentCount, len(segments))
		}
	}

	return
}

func MergeSegments(segments []*Segment) error {
	return nil
}
