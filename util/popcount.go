// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package util

// TODO asm implementation using the POPCNT instruction

// https://en.wikipedia.org/wiki/Hamming_weight#Efficient_implementation

// PopCount64 counts the number of bits set in an unsigned 64-bit integer.
func PopCount64(x uint64) int {
	x -= (x >> 1) & 0x5555555555555555                             // put count of each 2 bits into those 2 bits
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333) // put count of each 4 bits into those 4 bits
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f                        // put count of each 8 bits into those 8 bits
	return int((x * 0x0101010101010101) >> 56)                     // returns left 8 bits of x + (x<<8) + (x<<16) + (x<<24) + ...
}

// PopCount32 counts the number of bits set in an unsigned 32-bit integer.
func PopCount32(x uint32) int {
	x -= (x >> 1) & 0x55555555
	x = (x & 0x33333333) + ((x >> 2) & 0x33333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f
	return int((x * 0x01010101) >> 24)
}

// PopCount64Slice counts the number of bits set in a slice of unsigned 64-bit integers.
func PopCount64Slice(xs []uint64) int {
	var n int
	for _, x := range xs {
		n += PopCount64(x)
	}
	return n
}

// PopCount32Slice counts the number of bits set in a slice of unsigned 32-bit integers.
func PopCount32Slice(xs []uint32) int {
	var n int
	for _, x := range xs {
		n += PopCount32(x)
	}
	return n
}
