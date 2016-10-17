package index

import (
	"fmt"
	"log"
	"os"
	"path"
)

type SegmentMeta struct {
	NumDocs   int    `json:"numdocs"`   // Number of docs stored in the segment
	NumBlocks int    `json:"numblocks"` // Number of blocks in the segment's data file
	Size      int    `json:"size"`      // Size of the segment's data file
	Checksum  uint64 `json:"checksum"`  // Checksum of the segment's hash+docid pairs
}

type BlockInfo struct {
	FirstHash uint32 // First hash stored in the block
	Position  int    // Position of the block in the segment's data file
}

type Segment struct {
	ID     uint32
	meta   SegmentMeta
	blocks []BlockInfo
}

func NewSegment(id uint32) *Segment {
	return &Segment{ID: id}
}

func (s *Segment) AddBlock(block BlockInfo) {
	s.blocks = append(s.blocks, block)
}

func (s *Segment) MetaFileName() string {
	return fmt.Sprintf("segment-%v.meta.json", s.ID)
}

func (s *Segment) DataFileName() string {
	return fmt.Sprintf("segment-%v.data", s.ID)
}

func (s *Segment) RemoveFiles(dir string) {
	names := []string{
		path.Join(dir, s.MetaFileName()),
		path.Join(dir, s.DataFileName()),
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
