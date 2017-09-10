// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package vfs

import (
	"io"
	"os"
	"sync"
	"time"
)

type memFile struct {
	mu   sync.RWMutex
	data []byte
}

type memFS struct {
	mu    sync.RWMutex
	files map[string]*memFile
}

// CreateMemDir creates a FileSystem instance that only exists in memory.
// It does not use any OS-level file functions. It's useful for unit tests.
func CreateMemDir() FileSystem {
	return &memFS{
		files: make(map[string]*memFile),
	}
}

func (fs *memFS) Path() string {
	return ":memory:"
}

func (fs *memFS) String() string {
	return fs.Path()
}

func (fs *memFS) Close() error {
	return nil
}

func (fs *memFS) Lock(name string) (io.Closer, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, exists := fs.files[name]; exists {
		return nil, errLocked
	}
	fs.files[name] = &memFile{}
	return &memLock{fs: fs, name: name}, nil
}

func (fs *memFS) Rename(oldname, newname string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	file, exists := fs.files[oldname]
	if !exists {
		return &os.PathError{Op: "rename", Path: oldname, Err: os.ErrNotExist}
	}
	delete(fs.files, oldname)
	fs.files[newname] = file
	return nil
}

func (fs *memFS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, exists := fs.files[name]; !exists {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	delete(fs.files, name)
	return nil
}

func (fs *memFS) ReadDir() ([]os.FileInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	infos := make([]os.FileInfo, 0, len(fs.files))
	for name := range fs.files {
		infos = append(infos, &memFileInfo{name: name})
	}
	return infos, nil
}

func (fs *memFS) OpenFile(name string) (InputFile, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	file, exists := fs.files[name]
	if !exists {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}
	return &memInputFile{memFile: file}, nil
}

func (fs *memFS) CreateFile(name string, overwrite bool) (OutputFile, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	file, exists := fs.files[name]
	if exists {
		if !overwrite {
			return nil, &os.PathError{Op: "create", Path: name, Err: os.ErrExist}
		}
		file.Reset()
	} else {
		file = &memFile{}
		fs.files[name] = file
	}
	return &memOutputFile{memFile: file}, nil
}

func (fs *memFS) CreateAtomicFile(name string) (AtomicOutputFile, error) {
	file := &memFile{}
	return &memAtomicOutputFile{
		memOutputFile: memOutputFile{memFile: file},
		onCommit: func() error {
			fs.mu.Lock()
			defer fs.mu.Unlock()
			fs.files[name] = file
			return nil
		},
	}, nil
}

type memLock struct {
	fs   *memFS
	name string
}

func (lock *memLock) Close() error {
	return lock.fs.Remove(lock.name)
}

type memFileInfo struct {
	name string
	size int64
}

func (info *memFileInfo) Name() string       { return info.name }
func (info *memFileInfo) Size() int64        { return -1 }
func (info *memFileInfo) Mode() os.FileMode  { return os.ModePerm }
func (info *memFileInfo) ModTime() time.Time { return time.Now() }
func (info *memFileInfo) IsDir() bool        { return false }
func (info *memFileInfo) Sys() interface{}   { return nil }

type memOutputFile struct {
	*memFile
}

func (f *memFile) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = f.data[:0]
}

func (f *memOutputFile) Write(data []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = append(f.data, data...)
	return len(data), nil
}

func (f *memOutputFile) Sync() error {
	return nil
}

func (f *memOutputFile) Close() error {
	return nil
}

type memAtomicOutputFile struct {
	memOutputFile
	onCommit func() error
}

func (f *memAtomicOutputFile) Commit() error {
	err := f.onCommit()
	f.onCommit = func() error {
		return errCommitted
	}
	return err
}

type memInputFile struct {
	*memFile
	pos int64
}

func (f *memInputFile) Read(data []byte) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(data, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *memInputFile) ReadAt(data []byte, pos int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(data, f.data[pos:])
	if n < len(data) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memInputFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var pos int64
	switch whence {
	case io.SeekCurrent:
		pos = f.pos + offset
	case io.SeekStart:
		pos = offset
	case io.SeekEnd:
		pos = int64(len(f.data)) + offset
	default:
		return 0, os.ErrInvalid
	}
	if pos < 0 {
		return 0, os.ErrInvalid
	}
	f.pos = pos
	return pos, nil
}

func (f *memInputFile) Size() int64 {
	return int64(len(f.data))
}

func (f *memInputFile) Close() error {
	return nil
}
