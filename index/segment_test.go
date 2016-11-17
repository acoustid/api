// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSegment_Reader(t *testing.T) {
	var buf ItemBuffer
	buf.Add(1, []uint32{7, 8, 9})
	buf.Add(2, []uint32{3, 4, 5})

	segment, err := CreateSegment(vfs.CreateMemDir(), 0, buf.Reader())
	if assert.NoError(t, err, "failed to create segment") {
		items, err := ReadAllItems(segment.Reader())
		if assert.NoError(t, err, "failed to read items") {
			expected := []Item{{3, 2}, {4, 2}, {5, 2}, {7, 1}, {8, 1}, {9, 1}}
			assert.Equal(t, expected, items, "read items do not match")
		}
	}
}
