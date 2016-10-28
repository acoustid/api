package index

import (
	"bytes"
	"errors"
	"github.com/dchest/safefile"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type FileReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

type FileWriter interface {
	io.Writer
	io.Closer
	Commit() error
}

type Dir interface {
	Path() string
	OpenFile(name string) (FileReader, error)
	CreateFile(name string) (FileWriter, error)
	RemoveFile(name string) error
	ListFiles() ([]string, error)
}

type fsDir struct {
	path string
}

type TempDir struct {
	fsDir
}

var (
	ErrNotDirectory = errors.New("not a directory")
	ErrExist        = os.ErrExist
	ErrNotExist     = os.ErrNotExist
)

func IsExist(err error) bool {
	return os.IsExist(err)
}

func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

// OpenDirs opens a directory on the filesystem, optionally also create it if it does not exist.
func OpenDir(path string, create bool) (Dir, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	if stat, err := os.Stat(path); err != nil {
		if create && os.IsNotExist(err) {
			err = os.Mkdir(path, 0750)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if !stat.IsDir() {
		return nil, ErrNotDirectory
	}

	return &fsDir{path: path}, nil
}

func NewTempDir() (*TempDir, error) {
	path, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	log.Printf("created new temp directory at %v", path)
	return &TempDir{fsDir: fsDir{path: path}}, nil
}

func (d *TempDir) Close() {
	os.RemoveAll(d.Path())
}

func (d *fsDir) OpenFile(name string) (FileReader, error) {
	return os.Open(filepath.Join(d.path, name))
}

func (d *fsDir) CreateFile(name string) (FileWriter, error) {
	return safefile.Create(filepath.Join(d.path, name), 0644)
}

func (d *fsDir) RemoveFile(name string) error {
	err := os.Remove(filepath.Join(d.path, name))
	if err != nil && !os.IsNotExist(err){
		return err
	}
	return nil
}

func (d *fsDir) ListFiles() ([]string, error) {
	infos, err := ioutil.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		if !info.IsDir() {
			names = append(names, info.Name())
		}
	}
	return names, nil
}

func (d *fsDir) Path() string {
	return d.path
}

type memDir struct {
	entries map[string][]byte
}

type memFileReader struct {
	*bytes.Reader
}

type memFileWriter struct {
	bytes.Buffer
	dir  *memDir
	name string
}

// NewMemDir creates a temporary directory that only lives in the memory.
func NewMemDir() Dir {
	log.Print("created new memory directory")
	return &memDir{
		entries: make(map[string][]byte),
	}
}

func (d *memDir) OpenFile(name string) (FileReader, error) {
	entry, ok := d.entries[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	reader := &memFileReader{
		Reader: bytes.NewReader(entry),
	}
	return reader, nil
}

func (d *memDir) CreateFile(name string) (FileWriter, error) {
	_, ok := d.entries[name]
	if ok {
		return nil, os.ErrExist
	}
	writer := &memFileWriter{Buffer: bytes.Buffer{}, dir: d, name: name}
	return writer, nil
}

// Remove removes a file from the directory.
func (d *memDir) RemoveFile(name string) error {
	delete(d.entries, name)
	return nil
}

func (d *memDir) Path() string {
	return ""
}

func (d *memDir) ListFiles() ([]string, error) {
	names := make([]string, 0, len(d.entries))
	for name := range d.entries {
		names = append(names, name)
	}
	return names, nil
}

func (f *memFileReader) Close() error {
	return nil
}

func (f *memFileWriter) Commit() error {
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	f.dir.entries[f.name] = data
	return nil
}

func (f *memFileWriter) Close() error {
	return nil
}
