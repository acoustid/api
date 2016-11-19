// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"encoding/json"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"io"
	"log"
)

const ManifestFilename = "manifest.json"

type Manifest struct {
	ID              uint32              `json:"id"`
	BaseID          uint32              `json:"-"`
	NumDocs         int                 `json:"ndocs"`
	NumDeletedDocs  int                 `json:"ndeldocs,omitempty"`
	NumItems        int                 `json:"nitems"`
	Checksum        uint32              `json:"checksum"`
	Segments        map[uint32]*Segment `json:"segments"`
	addedSegments   map[uint32]struct{}
	removedSegments map[uint32]struct{}
	deleteAll       bool
}

func NewManifest() *Manifest {
	var m Manifest
	m.Reset()
	return &m
}

// Resets removes all segments from the manifest.
func (m *Manifest) Reset() {
	m.NumDocs = 0
	m.NumDeletedDocs = 0
	m.NumItems = 0
	m.Checksum = 0
	m.Segments = make(map[uint32]*Segment)
	m.addedSegments = make(map[uint32]struct{})
	m.removedSegments = make(map[uint32]struct{})
}

// Clone creates a copy of the manifest that can be updated independently.
func (m *Manifest) Clone() *Manifest {
	m2 := &Manifest{
		ID:              0,
		BaseID:          m.ID,
		NumDocs:         m.NumDocs,
		NumDeletedDocs:  m.NumDeletedDocs,
		NumItems:        m.NumItems,
		Checksum:        m.Checksum,
		Segments:        make(map[uint32]*Segment, len(m.Segments)),
		addedSegments:   make(map[uint32]struct{}),
		removedSegments: make(map[uint32]struct{}),
	}
	for id, segment := range m.Segments {
		m2.Segments[id] = segment.Clone()
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
			s2.DeleteMulti(s.docs)
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
	m.addedSegments[s.ID] = struct{}{}
}

// RemoveSegment removes a segment from the manifest and updates all internal stats.
func (m *Manifest) RemoveSegment(s *Segment) {
	m.NumDocs -= s.Meta.NumDocs
	m.NumDeletedDocs -= s.Meta.NumDeletedDocs
	m.NumItems -= s.Meta.NumItems
	m.Checksum -= s.Meta.Checksum
	delete(m.Segments, s.ID)
	m.removedSegments[s.ID] = struct{}{}
}

func (m *Manifest) Delete(docID uint32) {
	m.NumDeletedDocs = 0
	for _, segment := range m.Segments {
		if segment.Delete(docID) {
			log.Printf("deleted doc %v from segment %v", docID, segment.ID)
		}
		m.NumDeletedDocs += segment.NumDeletedDocs()
	}
}

func (m *Manifest) DeleteAll() {
	m.Reset()
	m.deleteAll = true
}

func (m *Manifest) Load(fs vfs.FileSystem, create bool) error {
	file, err := fs.OpenFile(ManifestFilename)
	if err != nil {
		if vfs.IsNotExist(err) && create {
			m.Reset()
			m.ID = 0
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

func (m *Manifest) rebase(base *Manifest) error {
	for _, s := range base.Segments {
		if s.dirty {
			return errors.New("base manifest can't have dirty segments")
		}
	}

	// Check for duplicate segments, this should normally not happen, but just in case.
	for id := range m.addedSegments {
		_, exists := base.Segments[id]
		if exists {
			return errors.Wrapf(errConflict, "segment %d was already added", id)
		}
	}

	// Check for segments removed twice. This is the usual conflict case we need to handle.
	for id := range m.removedSegments {
		_, exists := base.Segments[id]
		if !exists {
			return errors.Wrapf(errConflict, "segment %d was already removed", id)
		}
	}

	// Find segments in our manifest that have been updated.
	updatedSegments := make(map[uint32]struct{})
	for id, segment := range m.Segments {
		if segment.dirty {
			updatedSegments[id] = struct{}{}
		}
	}

	// 1) Find segments in the base manifest that had some docs deleted in between and apply the deletes to our segments.
	// 2) Remove segments that have been removed from the base manifest.
	for id, segment := range m.Segments {
		baseSegment, exists := base.Segments[id]
		if exists {
			if segment.UpdateID < baseSegment.UpdateID {
				if segment.dirty {
					segment.DeleteMulti(baseSegment.deletedDocs)
				} else {
					segment.deletedDocs = baseSegment.deletedDocs
					segment.Meta.NumDeletedDocs = baseSegment.Meta.NumDeletedDocs
				}
				segment.UpdateID = baseSegment.UpdateID
			}
		} else {
			_, exists = m.addedSegments[id]
			if !exists {
				m.RemoveSegment(segment)
			}
		}
	}

	// Copy segments that are only present in the base manifest, but delete duplicate docs from them.
	if !m.deleteAll {
		for id, segment := range base.Segments {
			_, exists := m.Segments[id]
			if !exists {
				segment := segment.Clone()
				if len(m.removedSegments) == 0 {
					for id2 := range m.addedSegments {
						segment.DeleteMulti(m.Segments[id2].docs)
					}
					for id2 := range updatedSegments {
						segment.DeleteMulti(m.Segments[id2].deletedDocs)
					}
				}
				m.addSegment(segment, false)
			}
		}
	}

	m.NumDeletedDocs = 0
	for _, s := range m.Segments {
		m.NumDeletedDocs += s.NumDeletedDocs()
	}

	m.BaseID = base.ID
	return nil
}

// Commit atomically saves the manifest, making sure any conflicts with the base manifest are either resolved or reported.
func (m *Manifest) Commit(fs vfs.FileSystem, id uint32, base *Manifest) error {
	if base != nil && base.ID > m.BaseID {
		err := m.rebase(base)
		if err != nil {
			return errors.Wrap(err, "rebase failed")
		}
	}

	for _, segment := range m.Segments {
		err := segment.SaveUpdate(fs, id)
		if err != nil {
			return errors.Wrap(err, "segment update failed")
		}
	}

	m.ID = id
	err := m.Save(fs)
	if err != nil {
		m.ID = 0
		return errors.Wrap(err, "save failed")
	}

	m.addedSegments = nil
	m.removedSegments = nil
	return nil
}

var errConflict = errors.New("conflicting manifests")

// IsConflict returns true if err was caused by a conflict.
func IsConflict(err error) bool {
	return errors.Cause(err) == errConflict
}
