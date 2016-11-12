package vfs

import (
	"io"
	"github.com/pkg/errors"
)

func WriteFile(fs FileSystem, name string, write func (w io.Writer) error) error {
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