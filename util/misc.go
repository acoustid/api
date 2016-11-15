// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package util

func MinUint32(a, b uint32) uint32 {
	if a <= b {
		return a
	}
	return b
}

func MaxUint32(a, b uint32) uint32 {
	if a >= b {
		return a
	}
	return b
}
