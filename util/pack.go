// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

// THIS FILE WAS AUTOMATICALLY GENERATED, DO NOT EDIT

package util

// PackUint1Slice converts an uint8 slice into a bit-packed uint1 slice.
func PackUint1Slice(dst []byte, src []uint8) int {
	n := 0
	for len(src) >= 8 {
		val := uint8(src[0]) | uint8(src[1])<<1 | uint8(src[2])<<2 | uint8(src[3])<<3 | uint8(src[4])<<4 | uint8(src[5])<<5 | uint8(src[6])<<6 | uint8(src[7])<<7
		dst[n] = uint8(val & 255)
		n += 1
		src = src[8:]
	}
	switch len(src) {
	case 7:
		val := uint8(src[0]) | uint8(src[1])<<1 | uint8(src[2])<<2 | uint8(src[3])<<3 | uint8(src[4])<<4 | uint8(src[5])<<5 | uint8(src[6])<<6
		dst[n] = uint8(val & 255)
		n += 1
	case 6:
		val := uint8(src[0]) | uint8(src[1])<<1 | uint8(src[2])<<2 | uint8(src[3])<<3 | uint8(src[4])<<4 | uint8(src[5])<<5
		dst[n] = uint8(val & 255)
		n += 1
	case 5:
		val := uint8(src[0]) | uint8(src[1])<<1 | uint8(src[2])<<2 | uint8(src[3])<<3 | uint8(src[4])<<4
		dst[n] = uint8(val & 255)
		n += 1
	case 4:
		val := uint8(src[0]) | uint8(src[1])<<1 | uint8(src[2])<<2 | uint8(src[3])<<3
		dst[n] = uint8(val & 255)
		n += 1
	case 3:
		val := uint8(src[0]) | uint8(src[1])<<1 | uint8(src[2])<<2
		dst[n] = uint8(val & 255)
		n += 1
	case 2:
		val := uint8(src[0]) | uint8(src[1])<<1
		dst[n] = uint8(val & 255)
		n += 1
	case 1:
		val := uint8(src[0])
		dst[n] = uint8(val & 255)
		n += 1
	}
	return n
}

// UnpackUint1Slice converts a bit-packed uint1 slice to an uint8 slice.
func UnpackUint1Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/1)
	n := 0
	for _, val := range src {
		d := dst[n : n+8 : len(dst)]
		d[0] = uint8((val >> 0) & 1)
		d[1] = uint8((val >> 1) & 1)
		d[2] = uint8((val >> 2) & 1)
		d[3] = uint8((val >> 3) & 1)
		d[4] = uint8((val >> 4) & 1)
		d[5] = uint8((val >> 5) & 1)
		d[6] = uint8((val >> 6) & 1)
		d[7] = uint8((val >> 7) & 1)
		n += 8
	}
	return dst
}

// PackUint2Slice converts an uint8 slice into a bit-packed uint2 slice.
func PackUint2Slice(dst []byte, src []uint8) int {
	n := 0
	for len(src) >= 4 {
		val := uint8(src[0]) | uint8(src[1])<<2 | uint8(src[2])<<4 | uint8(src[3])<<6
		dst[n] = uint8(val & 255)
		n += 1
		src = src[4:]
	}
	switch len(src) {
	case 3:
		val := uint8(src[0]) | uint8(src[1])<<2 | uint8(src[2])<<4
		dst[n] = uint8(val & 255)
		n += 1
	case 2:
		val := uint8(src[0]) | uint8(src[1])<<2
		dst[n] = uint8(val & 255)
		n += 1
	case 1:
		val := uint8(src[0])
		dst[n] = uint8(val & 255)
		n += 1
	}
	return n
}

// UnpackUint2Slice converts a bit-packed uint2 slice to an uint8 slice.
func UnpackUint2Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/2)
	n := 0
	for _, val := range src {
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 3)
		d[1] = uint8((val >> 2) & 3)
		d[2] = uint8((val >> 4) & 3)
		d[3] = uint8((val >> 6) & 3)
		n += 4
	}
	return dst
}

// PackUint3Slice converts an uint8 slice into a bit-packed uint3 slice.
func PackUint3Slice(dst []byte, src []uint8) int {
	n := 0
	for len(src) >= 8 {
		val := uint32(src[0]) | uint32(src[1])<<3 | uint32(src[2])<<6 | uint32(src[3])<<9 | uint32(src[4])<<12 | uint32(src[5])<<15 | uint32(src[6])<<18 | uint32(src[7])<<21
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		n += 3
		src = src[8:]
	}
	switch len(src) {
	case 7:
		val := uint32(src[0]) | uint32(src[1])<<3 | uint32(src[2])<<6 | uint32(src[3])<<9 | uint32(src[4])<<12 | uint32(src[5])<<15 | uint32(src[6])<<18
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		n += 3
	case 6:
		val := uint32(src[0]) | uint32(src[1])<<3 | uint32(src[2])<<6 | uint32(src[3])<<9 | uint32(src[4])<<12 | uint32(src[5])<<15
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		n += 3
	case 5:
		val := uint16(src[0]) | uint16(src[1])<<3 | uint16(src[2])<<6 | uint16(src[3])<<9 | uint16(src[4])<<12
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		n += 2
	case 4:
		val := uint16(src[0]) | uint16(src[1])<<3 | uint16(src[2])<<6 | uint16(src[3])<<9
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		n += 2
	case 3:
		val := uint16(src[0]) | uint16(src[1])<<3 | uint16(src[2])<<6
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		n += 2
	case 2:
		val := uint8(src[0]) | uint8(src[1])<<3
		dst[n] = uint8(val & 255)
		n += 1
	case 1:
		val := uint8(src[0])
		dst[n] = uint8(val & 255)
		n += 1
	}
	return n
}

// UnpackUint3Slice converts a bit-packed uint3 slice to an uint8 slice.
func UnpackUint3Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/3)
	n := 0
	for len(src) >= 3 {
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
		d := dst[n : n+8 : len(dst)]
		d[0] = uint8((val >> 0) & 7)
		d[1] = uint8((val >> 3) & 7)
		d[2] = uint8((val >> 6) & 7)
		d[3] = uint8((val >> 9) & 7)
		d[4] = uint8((val >> 12) & 7)
		d[5] = uint8((val >> 15) & 7)
		d[6] = uint8((val >> 18) & 7)
		d[7] = uint8((val >> 21) & 7)
		n += 8
		src = src[3:]
	}
	switch len(src) {
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<8
		d := dst[n : n+5 : len(dst)]
		d[0] = uint8((val >> 0) & 7)
		d[1] = uint8((val >> 3) & 7)
		d[2] = uint8((val >> 6) & 7)
		d[3] = uint8((val >> 9) & 7)
		d[4] = uint8((val >> 12) & 7)
		n += 5
	case 1:
		val := uint8(src[0])
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 7)
		d[1] = uint8((val >> 3) & 7)
		n += 2
	}
	return dst
}

// PackUint4Slice converts an uint8 slice into a bit-packed uint4 slice.
func PackUint4Slice(dst []byte, src []uint8) int {
	n := 0
	for len(src) >= 2 {
		val := uint8(src[0]) | uint8(src[1])<<4
		dst[n] = uint8(val & 255)
		n += 1
		src = src[2:]
	}
	switch len(src) {
	case 1:
		val := uint8(src[0])
		dst[n] = uint8(val & 255)
		n += 1
	}
	return n
}

// UnpackUint4Slice converts a bit-packed uint4 slice to an uint8 slice.
func UnpackUint4Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/4)
	n := 0
	for _, val := range src {
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 15)
		d[1] = uint8((val >> 4) & 15)
		n += 2
	}
	return dst
}

// PackUint5Slice converts an uint8 slice into a bit-packed uint5 slice.
func PackUint5Slice(dst []byte, src []uint8) int {
	n := 0
	for len(src) >= 8 {
		val := uint64(src[0]) | uint64(src[1])<<5 | uint64(src[2])<<10 | uint64(src[3])<<15 | uint64(src[4])<<20 | uint64(src[5])<<25 | uint64(src[6])<<30 | uint64(src[7])<<35
		d := dst[n : n+5 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		d[4] = uint8((val >> 32) & 255)
		n += 5
		src = src[8:]
	}
	switch len(src) {
	case 7:
		val := uint64(src[0]) | uint64(src[1])<<5 | uint64(src[2])<<10 | uint64(src[3])<<15 | uint64(src[4])<<20 | uint64(src[5])<<25 | uint64(src[6])<<30
		d := dst[n : n+5 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		d[4] = uint8((val >> 32) & 255)
		n += 5
	case 6:
		val := uint32(src[0]) | uint32(src[1])<<5 | uint32(src[2])<<10 | uint32(src[3])<<15 | uint32(src[4])<<20 | uint32(src[5])<<25
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		n += 4
	case 5:
		val := uint32(src[0]) | uint32(src[1])<<5 | uint32(src[2])<<10 | uint32(src[3])<<15 | uint32(src[4])<<20
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		n += 4
	case 4:
		val := uint32(src[0]) | uint32(src[1])<<5 | uint32(src[2])<<10 | uint32(src[3])<<15
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		n += 3
	case 3:
		val := uint16(src[0]) | uint16(src[1])<<5 | uint16(src[2])<<10
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		n += 2
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<5
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		n += 2
	case 1:
		val := uint8(src[0])
		dst[n] = uint8(val & 255)
		n += 1
	}
	return n
}

// UnpackUint5Slice converts a bit-packed uint5 slice to an uint8 slice.
func UnpackUint5Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/5)
	n := 0
	for len(src) >= 5 {
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32
		d := dst[n : n+8 : len(dst)]
		d[0] = uint8((val >> 0) & 31)
		d[1] = uint8((val >> 5) & 31)
		d[2] = uint8((val >> 10) & 31)
		d[3] = uint8((val >> 15) & 31)
		d[4] = uint8((val >> 20) & 31)
		d[5] = uint8((val >> 25) & 31)
		d[6] = uint8((val >> 30) & 31)
		d[7] = uint8((val >> 35) & 31)
		n += 8
		src = src[5:]
	}
	switch len(src) {
	case 4:
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16 | uint32(src[3])<<24
		d := dst[n : n+6 : len(dst)]
		d[0] = uint8((val >> 0) & 31)
		d[1] = uint8((val >> 5) & 31)
		d[2] = uint8((val >> 10) & 31)
		d[3] = uint8((val >> 15) & 31)
		d[4] = uint8((val >> 20) & 31)
		d[5] = uint8((val >> 25) & 31)
		n += 6
	case 3:
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 31)
		d[1] = uint8((val >> 5) & 31)
		d[2] = uint8((val >> 10) & 31)
		d[3] = uint8((val >> 15) & 31)
		n += 4
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<8
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 31)
		d[1] = uint8((val >> 5) & 31)
		d[2] = uint8((val >> 10) & 31)
		n += 3
	case 1:
		val := uint8(src[0])
		d := dst[n : n+1 : len(dst)]
		d[0] = uint8((val >> 0) & 31)
		n += 1
	}
	return dst
}

// PackUint6Slice converts an uint8 slice into a bit-packed uint6 slice.
func PackUint6Slice(dst []byte, src []uint8) int {
	n := 0
	for len(src) >= 4 {
		val := uint32(src[0]) | uint32(src[1])<<6 | uint32(src[2])<<12 | uint32(src[3])<<18
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		n += 3
		src = src[4:]
	}
	switch len(src) {
	case 3:
		val := uint32(src[0]) | uint32(src[1])<<6 | uint32(src[2])<<12
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		n += 3
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<6
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		n += 2
	case 1:
		val := uint8(src[0])
		dst[n] = uint8(val & 255)
		n += 1
	}
	return n
}

// UnpackUint6Slice converts a bit-packed uint6 slice to an uint8 slice.
func UnpackUint6Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/6)
	n := 0
	for len(src) >= 3 {
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 63)
		d[1] = uint8((val >> 6) & 63)
		d[2] = uint8((val >> 12) & 63)
		d[3] = uint8((val >> 18) & 63)
		n += 4
		src = src[3:]
	}
	switch len(src) {
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<8
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 63)
		d[1] = uint8((val >> 6) & 63)
		n += 2
	case 1:
		val := uint8(src[0])
		d := dst[n : n+1 : len(dst)]
		d[0] = uint8((val >> 0) & 63)
		n += 1
	}
	return dst
}

// PackUint7Slice converts an uint8 slice into a bit-packed uint7 slice.
func PackUint7Slice(dst []byte, src []uint8) int {
	n := 0
	for len(src) >= 8 {
		val := uint64(src[0]) | uint64(src[1])<<7 | uint64(src[2])<<14 | uint64(src[3])<<21 | uint64(src[4])<<28 | uint64(src[5])<<35 | uint64(src[6])<<42 | uint64(src[7])<<49
		d := dst[n : n+7 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		d[4] = uint8((val >> 32) & 255)
		d[5] = uint8((val >> 40) & 255)
		d[6] = uint8((val >> 48) & 255)
		n += 7
		src = src[8:]
	}
	switch len(src) {
	case 7:
		val := uint64(src[0]) | uint64(src[1])<<7 | uint64(src[2])<<14 | uint64(src[3])<<21 | uint64(src[4])<<28 | uint64(src[5])<<35 | uint64(src[6])<<42
		d := dst[n : n+7 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		d[4] = uint8((val >> 32) & 255)
		d[5] = uint8((val >> 40) & 255)
		d[6] = uint8((val >> 48) & 255)
		n += 7
	case 6:
		val := uint64(src[0]) | uint64(src[1])<<7 | uint64(src[2])<<14 | uint64(src[3])<<21 | uint64(src[4])<<28 | uint64(src[5])<<35
		d := dst[n : n+6 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		d[4] = uint8((val >> 32) & 255)
		d[5] = uint8((val >> 40) & 255)
		n += 6
	case 5:
		val := uint64(src[0]) | uint64(src[1])<<7 | uint64(src[2])<<14 | uint64(src[3])<<21 | uint64(src[4])<<28
		d := dst[n : n+5 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		d[4] = uint8((val >> 32) & 255)
		n += 5
	case 4:
		val := uint32(src[0]) | uint32(src[1])<<7 | uint32(src[2])<<14 | uint32(src[3])<<21
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		d[3] = uint8((val >> 24) & 255)
		n += 4
	case 3:
		val := uint32(src[0]) | uint32(src[1])<<7 | uint32(src[2])<<14
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		d[2] = uint8((val >> 16) & 255)
		n += 3
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<7
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 255)
		d[1] = uint8((val >> 8) & 255)
		n += 2
	case 1:
		val := uint8(src[0])
		dst[n] = uint8(val & 255)
		n += 1
	}
	return n
}

// UnpackUint7Slice converts a bit-packed uint7 slice to an uint8 slice.
func UnpackUint7Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/7)
	n := 0
	for len(src) >= 7 {
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32 | uint64(src[5])<<40 | uint64(src[6])<<48
		d := dst[n : n+8 : len(dst)]
		d[0] = uint8((val >> 0) & 127)
		d[1] = uint8((val >> 7) & 127)
		d[2] = uint8((val >> 14) & 127)
		d[3] = uint8((val >> 21) & 127)
		d[4] = uint8((val >> 28) & 127)
		d[5] = uint8((val >> 35) & 127)
		d[6] = uint8((val >> 42) & 127)
		d[7] = uint8((val >> 49) & 127)
		n += 8
		src = src[7:]
	}
	switch len(src) {
	case 6:
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32 | uint64(src[5])<<40
		d := dst[n : n+6 : len(dst)]
		d[0] = uint8((val >> 0) & 127)
		d[1] = uint8((val >> 7) & 127)
		d[2] = uint8((val >> 14) & 127)
		d[3] = uint8((val >> 21) & 127)
		d[4] = uint8((val >> 28) & 127)
		d[5] = uint8((val >> 35) & 127)
		n += 6
	case 5:
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32
		d := dst[n : n+5 : len(dst)]
		d[0] = uint8((val >> 0) & 127)
		d[1] = uint8((val >> 7) & 127)
		d[2] = uint8((val >> 14) & 127)
		d[3] = uint8((val >> 21) & 127)
		d[4] = uint8((val >> 28) & 127)
		n += 5
	case 4:
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16 | uint32(src[3])<<24
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 127)
		d[1] = uint8((val >> 7) & 127)
		d[2] = uint8((val >> 14) & 127)
		d[3] = uint8((val >> 21) & 127)
		n += 4
	case 3:
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 127)
		d[1] = uint8((val >> 7) & 127)
		d[2] = uint8((val >> 14) & 127)
		n += 3
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<8
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 127)
		d[1] = uint8((val >> 7) & 127)
		n += 2
	case 1:
		val := uint8(src[0])
		d := dst[n : n+1 : len(dst)]
		d[0] = uint8((val >> 0) & 127)
		n += 1
	}
	return dst
}
