// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package vfs

import (
	"testing"
)

func TestCreateMemDir(t *testing.T) {
	fs := CreateMemDir()
	defer fs.Close()
}
