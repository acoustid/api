// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"encoding/json"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"io"
	"github.com/acoustid/go-acoustid/util/bitset"
)

const ManifestFilename = "manifest.json"

type Manifest struct {
	ID             uint32              `json:"id"`
	BaseID         uint32              `json:"-"`
	NumDocs        int                 `json:"ndocs"`
	NumDeletedDocs int                 `json:"ndeldocs,omitempty"`
	NumItems       int                 `json:"nitems"`
	Checksum       uint32              `json:"checksum"`
	Segments       map[uint32]*Segment `json:"segments"`
}

// Resets removes all segments from the manifest.
func (m *Manifest) Reset() {
	m.NumDocs = 0
	m.NumDeletedDocs = 0
	m.NumItems = 0
	m.Checksum = 0
	m.Segments = make(map[uint32]*Segment)
}

// Clone creates a copy of the manifest that can be updated independently.
func (m *Manifest) Clone() *Manifest {
	m2 := &Manifest{
		ID:             0,
		BaseID:         m.ID,
		NumDocs:        m.NumDocs,
		NumDeletedDocs: m.NumDeletedDocs,
		NumItems:       m.NumItems,
		Checksum:       m.Checksum,
		Segments:       make(map[uint32]*Segment, len(m.Segments)),
	}
	for _, s := range m.Segments {
		m2.Segments[s.ID] = s.Clone()
	}
	return m2
}

func (m *Manifest) addSegment(s *Segment, dedupe bool) {
	m.NumDocs += s.Meta.NumDocs
	m.NumItems += s.Meta.NumItems
	m.Checksum += s.Meta.Checksum
	if dedupe {
		m.NumDeletedDocs = s.Meta.NumDeletedDocs
		for _, s2 := range m.Segments {
			s2.DeleteMulti(&s.docs)
			m.NumDeletedDocs += s2.Meta.NumDeletedDocs
		}
	} else {
		m.NumDeletedDocs += s.Meta.NumDeletedDocs
	}
	m.Segments[s.ID] = s
}


// AddSegment adds a new segment to the manifest and updates all internal stats.
func (m *Manifest) AddSegment(s *Segment) {
	m.addSegment(s, true)
}

// RemoveSegment removes a segment from the manifest and updates all internal stats.
func (m *Manifest) RemoveSegment(s *Segment) {
	m.NumDocs -= s.Meta.NumDocs
	m.NumDeletedDocs -= s.Meta.NumDeletedDocs
	m.NumItems -= s.Meta.NumItems
	m.Checksum -= s.Meta.Checksum
	delete(m.Segments, s.ID)
}

func (m *Manifest) UpdateStats() {
	m.NumDocs = 0
	m.NumDeletedDocs = 0
	m.NumItems = 0
	for _, s := range m.Segments {
		m.NumDocs += s.NumDocs()
		m.NumDeletedDocs += s.NumDeletedDocs()
		m.NumItems += s.NumItems()
	}
}

func (m *Manifest) Load(fs vfs.FileSystem, create bool) error {
	file, err := fs.OpenFile(ManifestFilename)
	if err != nil {
		if vfs.IsNotExist(err) && create {
			m.Reset()
			m.ID = 1
			return m.Save(fs)
		}
		return errors.Wrap(err, "open failed")
	}
	err = json.NewDecoder(file).Decode(m)
	if err != nil {
		return errors.Wrap(err, "decode failed")
	}
	return nil
}

func (m *Manifest) Save(fs vfs.FileSystem) error {
	return vfs.WriteFile(fs, ManifestFilename, func(w io.Writer) error {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(m)
	})
}

// Rebase updates the manifest to include all changes from base, as if it was originally cloned from that.
func (m *Manifest) Rebase(base *Manifest) error {
	if m.BaseID == base.ID {
		return nil
	}

	if m.ID != 0 {
		return errors.New("can't rebase committed manifest")
	}

	for _, s := range base.Segments {
		if s.dirty {
			return errors.New("base manifest can't have dirty segments")
		}
	}

	// Find segments in the base manifest that had some docs deleted in between and apply the deletes to our segments.
	for _, s := range m.Segments {
		s2 := base.Segments[s.ID]
		if s2 != nil && s2.deletedDocs != nil && s.UpdateID != s2.UpdateID {
			if !s.dirty {
				s.deletedDocs = s2.deletedDocs
				s.Meta.NumDeletedDocs = s2.Meta.NumDeletedDocs
				s.UpdateID = s2.UpdateID
				continue
			}
			s.DeleteMulti(s2.deletedDocs)
		}
	}

	// Build a set of docs added during the transaction.
	addedDocs := bitset.NewSparseBitSet(0)
	for _, s := range m.Segments {
		_, exists := base.Segments[s.ID]
		if !exists {
			addedDocs.Union(&s.docs)
		}
	}

	// Copy segments that are only present in the base manifest, but delete duplicate docs from them.
	for _, s := range base.Segments {
		_, exists := m.Segments[s.ID]
		if !exists {
			s2 := s.Clone()
			s2.DeleteMulti(addedDocs)
			m.addSegment(s2, false)
		}
	}

	m.NumDeletedDocs = 0
	for _, s := range m.Segments {
		m.NumDeletedDocs += s.NumDeletedDocs()
	}

	m.BaseID = base.ID

	return nil
}
