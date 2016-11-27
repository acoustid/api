// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"bytes"
	"fmt"
	"github.com/acoustid/go-acoustid/util/intset"
	"github.com/pkg/errors"
	"go4.org/sort"
	"log"
	"math"
	"strings"
)

// Merge provides information necessary to perform a merge operation, resulting in one new segment.
type Merge struct {
	Segments   []*Segment
	Score      float64
	Size       int
	newSegment *Segment
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

func (m *Merge) Run(db *DB) error {
	sort.Slice(m.Segments, func(i, j int) bool {
		return m.Segments[i].ID < m.Segments[j].ID
	})

	var ids []string
	var readers []ItemReader
	for _, segment := range m.Segments {
		ids = append(ids, fmt.Sprintf("%v", segment.ID))
		readers = append(readers, segment.Reader())
	}

	segment, err := db.createSegment(MergeItemReaders(readers...))
	if err != nil {
		return errors.Wrap(err, "segment merge failed")
	}

	log.Printf("merged segments %v into %v", strings.Join(ids, ", "), segment.ID)
	m.newSegment = segment

	return db.commit(m.prepareCommit)
}

func (m *Merge) prepareCommit(base *Manifest) (*Manifest, error) {
	manifest := base.Clone()

	var deletedDocs *intset.SparseBitSet
	for _, oldSegment := range m.Segments {
		segment, exists := manifest.Segments[oldSegment.ID]
		if !exists {
			return nil, errors.Wrapf(errConflict, "segment %v no longer exists", oldSegment.ID)
		}
		if segment.UpdateID != oldSegment.UpdateID {
			if deletedDocs == nil {
				deletedDocs = segment.deletedDocs.Clone()
			} else {
				deletedDocs.Union(segment.deletedDocs)
			}
		}
		manifest.RemoveSegment(segment)
	}
	if deletedDocs != nil {
		m.newSegment.DeleteMulti(deletedDocs)
	}

	_, exists := manifest.Segments[m.newSegment.ID]
	if exists {
		return nil, errors.Wrapf(errConflict, "segment %v already exists", m.newSegment.ID)
	}
	manifest.addSegment(m.newSegment, false)

	return manifest, nil
}

// MergePolicy determines a sequence of merge operations.
type MergePolicy interface {
	FindBestMerge(manifest *Manifest, maxSize int) *Merge
}

// TieredMergePolicy is an adaptation of the algorithm from Lucene's TieredMergePolicy written by Michael McCandless.
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
}

// NewTieredMergePolicy creates a new TieredMergePolicy instance with the default options.
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

func (mp *TieredMergePolicy) FindBestMerge(manifest *Manifest, maxSize int) *Merge {
	if len(manifest.Segments) == 0 {
		return nil
	}
	if maxSize == 0 {
		maxSize = mp.MaxMergedSegmentSize
	}

	// Filter our segments that are over-sized and we could not potentially merge them.
	segments := make([]*Segment, 0, len(manifest.Segments))
	for _, segment := range manifest.Segments {
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

	if len(segments) <= allowedSegmentCount {
		return nil
	}

	var bestMerge *Merge
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

		if bestMerge == nil || merge.Score < bestMerge.Score {
			bestMerge = &merge
		}
	}
	return bestMerge
}
