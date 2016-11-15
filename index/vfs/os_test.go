// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package vfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenDir(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	root := filepath.Join(tmpDir, "foo", "bar")

	fs, err := OpenDir(root, false)
	if assert.Error(t, err) {
		assert.True(t, IsNotExist(err))
	}

	fs, err = OpenDir(root, true)
	if assert.NoError(t, err) {
		defer fs.Close()
		assert.Equal(t, root, fs.Path())
	}

	fs, err = OpenDir(filepath.Join(tmpDir, ".", "foo", "..", "foo", "bar"), false)
	if assert.NoError(t, err) {
		defer fs.Close()
		assert.Equal(t, root, fs.Path(), "path should be normalized")
	}
}

func TestCreateTempDir(t *testing.T) {
	fs, err := CreateTempDir()
	require.NoError(t, err)
	defer fs.Close()

	assert.True(t, strings.HasPrefix(fs.Path(), os.TempDir()))
}
