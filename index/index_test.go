package index

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestIndex(t *testing.T) {
	dir, err := ioutil.TempDir("", "aindex_test")
	if err != nil {
		t.Skip("could not create temporary directory to run index tests")
	}
	defer os.RemoveAll(dir)

	t.Run("idx1", func(t *testing.T) {
		idx, err := Open(path.Join(dir, "idx1"))
		if assert.NoError(t, err) {
			idx.Add(913428, []uint32{0xdcfc2563, 0xdcbc2421, 0xddbc3420, 0xdd9c1530, 0xdf9c6d40, 0x4f4ce540, 0x4f0ea5c0})
			idx.Close()
		}
	})
}
