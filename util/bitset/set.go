// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package bitset

type Uint32Set interface {
	Add(i uint32)
	Remove(i uint32)
	Contains(i uint32) bool
}
