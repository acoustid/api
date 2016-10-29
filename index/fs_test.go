package index

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"sort"
	"testing"
)

func TestMemDir_Write(t *testing.T) {
	d := NewMemDir()
	f, err := d.CreateFile("foo")
	if assert.NoError(t, err) {
		_, err := io.WriteString(f, "hello")
		assert.NoError(t, err)
		assert.NoError(t, f.Commit())
		assert.NoError(t, f.Close())
		f, err := d.OpenFile("foo")
		if assert.NoError(t, err) {
			b, err := ioutil.ReadAll(f)
			if assert.NoError(t, err) {
				assert.Equal(t, "hello", string(b))
			}
		}
	}
}

func TestMemDir_WriteWithoutCommit(t *testing.T) {
	d := NewMemDir()
	f, err := d.CreateFile("foo")
	if assert.NoError(t, err) {
		_, err := io.WriteString(f, "hello")
		assert.NoError(t, err)
		assert.NoError(t, f.Close())
		_, err = d.OpenFile("foo")
		assert.Error(t, err)
	}
}

func TestDir_List(t *testing.T) {
	check := func(t *testing.T, d Dir) {
		f1, err := d.CreateFile("foo")
		require.NoError(t, err)
		f1.Commit()
		f1.Close()

		f2, err := d.CreateFile("bar")
		require.NoError(t, err)
		f2.Commit()
		f2.Close()

		f3, err := d.CreateFile("baz")
		require.NoError(t, err)
		f3.Close()

		files, err := d.ListFiles()
		require.NoError(t, err)
		sort.Strings(files)
		require.Equal(t, []string{"bar", "foo"}, files)
	}

	t.Run("MemDir", func(t *testing.T) {
		d := NewMemDir()
		check(t, d)
	})

	t.Run("FsDir", func(t *testing.T) {
		d, err := NewTempDir()
		require.NoError(t, err)
		defer d.Close()
		check(t, d)
	})
}
