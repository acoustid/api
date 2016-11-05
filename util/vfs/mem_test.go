package vfs

import (
	"testing"
)

func TestCreateMemDir(t *testing.T) {
	fs := CreateMemDir()
	defer fs.Close()
}
