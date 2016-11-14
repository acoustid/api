package index

import (
	"github.com/acoustid/go-acoustid/index/vfs"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestSegment_Reader(t *testing.T) {
	var buf ItemBuffer
	buf.Add(1, []uint32{7, 8, 9})
	buf.Add(2, []uint32{3, 4, 5})

	fs := vfs.CreateMemDir()
	segment, err := CreateSegment(fs, 0, buf.Reader())
	if assert.NoError(t, err, "failed to create segment") {
		reader := segment.Reader()
		var items []Item
		for {
			block, err := reader.ReadBlock()
			if err == io.EOF {
				break
			}
			if assert.NoError(t, err, "failed to read block") {
				items = append(items, block...)
			}
		}
		expected := []Item{{3, 2}, {4, 2}, {5, 2}, {7, 1}, {8, 1}, {9, 1}}
		assert.Equal(t, expected, items)
	}
}
