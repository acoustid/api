package index

import (
	"fmt"
	"os"
	"path"
	"log"
)

type SegmentMeta struct {
	// ID of the segment
	ID uint32 `json:"id"`
	// Size of the segment's data file
	Size int `json:"size"`
	// Checksum of the segment's hash+docid pairs
	Checksum uint64 `json:"checksum"`
	// Number of docs stored in the segment
	NumDocs int `json:"numdocs"`
}

type BlockInfo struct {
	// First hash stored in the block
	FirstHash uint32
	// Position of the block in the segment's data file
	Position int
}

type Segment struct {
	meta SegmentMeta
	blocks []BlockInfo
}

func NewSegment(id uint32) *Segment {
	return &Segment{
		meta: SegmentMeta{
			ID: id,
		},
	}
}

func (s *Segment) AddBlock(block BlockInfo) {
	s.blocks = append(s.blocks, block)
}

func (s *Segment) MetaFilename() string {
	return fmt.Sprintf("segment-%v.meta.json", s.meta.ID)
}

func (s *Segment) DataFilename() string {
	return fmt.Sprintf("segment-%v.data", s.meta.ID)
}

func (s *Segment) RemoveFiles(dir string) {
	names := []string {
		path.Join(dir, s.MetaFilename()),
		path.Join(dir, s.DataFilename()),
	}
	for _, name := range names {
		err := os.Remove(name)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Printf("failed to remove segment file %v (%v)", name, err)
			}
		} else {
			log.Printf("removed segment file %v", name)
		}
	}
}