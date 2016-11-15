// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"encoding/json"
	"github.com/acoustid/go-acoustid/index/vfs"
	"github.com/pkg/errors"
	"io"
)

const ManifestFilename = "manifest.json"

type Manifest struct {
	ID       uint32     `json:"id"`
	NumDocs  int        `json:"ndocs"`
	NumItems int        `json:"nitems"`
	Checksum uint32     `json:"checksum"`
	Segments []*Segment `json:"segments"`
}

// Resets removes all segments from the manifest.
func (m *Manifest) Reset() {
	m.NumDocs = 0
	m.NumItems = 0
	m.Checksum = 0
	m.Segments = m.Segments[:0]
}

// Clone creates a copy of the manifest that can be updated independently.
func (m *Manifest) Clone() *Manifest {
	m2 := &Manifest{
		ID:       m.ID,
		NumDocs:  m.NumDocs,
		NumItems: m.NumItems,
		Checksum: m.Checksum,
		Segments: make([]*Segment, len(m.Segments)),
	}
	for i, s := range m.Segments {
		m2.Segments[i] = s.Clone()
	}
	return m2
}

// AddSegment adds a new segment to the manifest and updates all internal stats.
func (m *Manifest) AddSegment(s *Segment) {
	m.NumDocs += s.Meta.NumDocs
	m.NumItems += s.Meta.NumItems
	m.Checksum += s.Meta.Checksum
	m.Segments = append(m.Segments, s)
}

// RemoveSegment removes a segment from the manifest and updates all internal stats.
func (m *Manifest) RemoveSegment(s *Segment) {
	segments := m.Segments
	m.Segments = m.Segments[:0]
	for _, s2 := range segments {
		if s2 == s {
			m.NumDocs -= s2.Meta.NumDocs
			m.NumItems -= s2.Meta.NumItems
			m.Checksum -= s2.Meta.Checksum
		} else {
			m.Segments = append(m.Segments, s2)
		}
	}
}

func (m *Manifest) Load(fs vfs.FileSystem, create bool) error {
	file, err := fs.OpenFile(ManifestFilename)
	if err != nil {
		if vfs.IsNotExist(err) && create {
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

func (m *Manifest) WriteTo(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(m)
}

func (m *Manifest) Save(fs vfs.FileSystem) error {
	return vfs.WriteFile(fs, ManifestFilename, m.WriteTo)
}
