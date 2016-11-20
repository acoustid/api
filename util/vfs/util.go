// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package vfs

import (
	"github.com/pkg/errors"
	"io"
)

// WriteFile atomically writes the result of the write function to a file.
// If the write function returns a non-nil error, the file is not saved.
func WriteFile(fs FileSystem, name string, write func(w io.Writer) error) error {
	file, err := fs.CreateAtomicFile(name)
	if err != nil {
		return errors.Wrap(err, "create failed")
	}
	defer file.Close()

	err = write(file)
	if err != nil {
		return errors.Wrap(err, "write failed")
	}

	err = file.Commit()
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	return nil
}
