// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package vfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"testing"
)

func RunFileSystemTests(t *testing.T, testFn func(t *testing.T, fs FileSystem)) {
	t.Run("FS=OS", func(t *testing.T) {
		if fs, err := CreateTempDir(); assert.NoError(t, err) {
			defer fs.Close()
			testFn(t, fs)
		}
	})
	t.Run("FS=Mem", func(t *testing.T) {
		fs := CreateMemDir()
		defer fs.Close()
		testFn(t, fs)
	})
}

func TestLock(t *testing.T) {
	RunFileSystemTests(t, func(t *testing.T, fs FileSystem) {
		lock, err := fs.Lock("test.lock")
		if assert.NoError(t, err) {
			lock.Close()
		}
	})
}

func TestReadDir(t *testing.T) {
	RunFileSystemTests(t, func(t *testing.T, fs FileSystem) {
		entries, err := fs.ReadDir()
		if assert.NoError(t, err) {
			assert.Empty(t, entries)
		}
	})
}

func TestOpenCreateFile(t *testing.T) {
	RunFileSystemTests(t, func(t *testing.T, fs FileSystem) {
		if file, err := fs.OpenFile("foo"); assert.Error(t, err) {
			assert.True(t, IsNotExist(err))
			assert.Nil(t, file)
		}
		if file, err := fs.CreateFile("foo", false); assert.NoError(t, err) {
			defer file.Close()
			file.Write([]byte("hello"))
		}
		if file, err := fs.CreateFile("foo", false); assert.Error(t, err) {
			assert.True(t, IsExist(err))
			assert.Nil(t, file)
		}
		if file, err := fs.CreateFile("foo", true); assert.NoError(t, err) {
			defer file.Close()
			file.Write([]byte("world"))
		}
		if file, err := fs.OpenFile("foo"); assert.NoError(t, err) {
			data, err := ioutil.ReadAll(file)
			if assert.NoError(t, err) {
				assert.Equal(t, "world", string(data))
			}
		}
	})
}

func TestFileReadSeek(t *testing.T) {
	RunFileSystemTests(t, func(t *testing.T, fs FileSystem) {
		if file, err := fs.CreateFile("foo", false); assert.NoError(t, err) {
			defer file.Close()
			file.Write([]byte("0123456789"))
		}
		if file, err := fs.OpenFile("foo"); assert.NoError(t, err) {
			defer file.Close()
			buf := make([]byte, 10)
			if n, err := file.Read(buf[:2]); assert.NoError(t, err) {
				assert.Equal(t, 2, n)
				assert.Equal(t, "01", string(buf[:n]))
			}
			if n, err := file.Read(buf[:3]); assert.NoError(t, err) {
				assert.Equal(t, 3, n)
				assert.Equal(t, "234", string(buf[:n]))
			}
			if n, err := file.Read(buf); assert.NoError(t, err) {
				assert.Equal(t, 5, n)
				assert.Equal(t, "56789", string(buf[:n]))
			}
			if n, err := file.Read(buf); assert.Error(t, err) {
				assert.Equal(t, err, io.EOF)
				assert.Zero(t, n)
			}
			if n, err := file.ReadAt(buf[:3], 1); assert.NoError(t, err) {
				assert.Equal(t, 3, n)
				assert.Equal(t, "123", string(buf[:n]))
			}
			if n, err := file.Read(buf); assert.Error(t, err) {
				assert.Equal(t, err, io.EOF)
				assert.Zero(t, n)
			}
			if n, err := file.ReadAt(buf[:3], 7); assert.NoError(t, err) {
				assert.Equal(t, 3, n)
				assert.Equal(t, "789", string(buf[:n]))
			}
			if n, err := file.ReadAt(buf[:3], 8); assert.Error(t, err) {
				assert.Equal(t, err, io.EOF)
				assert.Equal(t, 2, n)
				assert.Equal(t, "89", string(buf[:n]))
			}
			if n, err := file.ReadAt(buf[:3], 100); assert.Error(t, err) {
				assert.Equal(t, err, io.EOF)
				assert.Zero(t, n)
			}
			if pos, err := file.Seek(1, io.SeekStart); assert.NoError(t, err) {
				assert.Equal(t, int64(1), pos)
			}
			if n, err := file.Read(buf[:2]); assert.NoError(t, err) {
				assert.Equal(t, 2, n)
				assert.Equal(t, "12", string(buf[:n]))
			}
			if pos, err := file.Seek(-3, io.SeekCurrent); assert.NoError(t, err) {
				assert.Equal(t, int64(0), pos)
			}
			if n, err := file.Read(buf[:2]); assert.NoError(t, err) {
				assert.Equal(t, 2, n)
				assert.Equal(t, "01", string(buf[:n]))
			}
			if pos, err := file.Seek(-3, io.SeekCurrent); assert.Error(t, err) {
				assert.Equal(t, int64(0), pos)
			}
			if n, err := file.Read(buf[:2]); assert.NoError(t, err) {
				assert.Equal(t, 2, n)
				assert.Equal(t, "23", string(buf[:n]))
			}
			if pos, err := file.Seek(-2, io.SeekEnd); assert.NoError(t, err) {
				assert.Equal(t, int64(8), pos)
			}
			if n, err := file.Read(buf[:2]); assert.NoError(t, err) {
				assert.Equal(t, 2, n)
				assert.Equal(t, "89", string(buf[:n]))
			}
		}
	})
}

func TestRename(t *testing.T) {
	RunFileSystemTests(t, func(t *testing.T, fs FileSystem) {
		file, err := fs.CreateFile("foo", false)
		require.NoError(t, err)
		file.Write([]byte("hello"))
		file.Close()

		err = fs.Rename("foo", "bar")
		require.NoError(t, err)

		if file, err := fs.OpenFile("foo"); assert.Error(t, err) {
			assert.True(t, IsNotExist(err))
			assert.Nil(t, file)
		}

		if file, err := fs.OpenFile("bar"); assert.NoError(t, err) {
			if data, err := ioutil.ReadAll(file); assert.NoError(t, err) {
				assert.Equal(t, "hello", string(data))
			}
		}

		err = fs.Rename("baz", "bar")
		require.Error(t, err)
		require.True(t, IsNotExist(err))

		file, err = fs.CreateFile("baz", false)
		require.NoError(t, err)
		file.Write([]byte("world"))
		file.Close()

		err = fs.Rename("baz", "bar")
		require.NoError(t, err)

		if file, err := fs.OpenFile("bar"); assert.NoError(t, err) {
			if data, err := ioutil.ReadAll(file); assert.NoError(t, err) {
				assert.Equal(t, "world", string(data))
			}
		}
	})
}

func TestRemove(t *testing.T) {
	RunFileSystemTests(t, func(t *testing.T, fs FileSystem) {
		err := fs.Remove("foo")
		require.Error(t, err)
		require.True(t, IsNotExist(err))

		file, err := fs.CreateFile("foo", false)
		require.NoError(t, err)
		file.Write([]byte("hello"))
		file.Close()

		err = fs.Remove("foo")
		require.NoError(t, err)
	})
}

func TestCreateAtomicFile(t *testing.T) {
	RunFileSystemTests(t, func(t *testing.T, fs FileSystem) {
		file, err := fs.CreateAtomicFile("foo")
		require.NoError(t, err)
		file.Write([]byte("hello"))
		file.Close()

		_, err = fs.OpenFile("foo")
		require.Error(t, err)
		require.True(t, IsNotExist(err))

		file, err = fs.CreateAtomicFile("foo")
		require.NoError(t, err)
		file.Write([]byte("hello"))

		_, err = fs.OpenFile("foo")
		require.Error(t, err)
		require.True(t, IsNotExist(err))

		err = file.Commit()
		require.NoError(t, err)
		file.Close()

		_, err = fs.OpenFile("foo")
		require.NoError(t, err)
	})
}
