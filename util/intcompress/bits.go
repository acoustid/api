package intcompress

// UnpackUint1Slice converts a bit-packed uint1 slice to an uint8 slice.
func UnpackUint1Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/1)
	n := 0
	for len(src) >= 1 {
		val := uint8(src[0])
		d := dst[n : n+8 : len(dst)]
		d[0] = uint8((val >> 0) & 0x1)
		d[1] = uint8((val >> 1) & 0x1)
		d[2] = uint8((val >> 2) & 0x1)
		d[3] = uint8((val >> 3) & 0x1)
		d[4] = uint8((val >> 4) & 0x1)
		d[5] = uint8((val >> 5) & 0x1)
		d[6] = uint8((val >> 6) & 0x1)
		d[7] = uint8((val >> 7) & 0x1)
		n += 8
		src = src[1:]
	}
	return dst
}

// UnpackUint2Slice converts a bit-packed uint2 slice to an uint8 slice.
func UnpackUint2Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/2)
	n := 0
	for len(src) >= 1 {
		val := uint8(src[0])
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 0x3)
		d[1] = uint8((val >> 2) & 0x3)
		d[2] = uint8((val >> 4) & 0x3)
		d[3] = uint8((val >> 6) & 0x3)
		n += 4
		src = src[1:]
	}
	return dst
}

// UnpackUint3Slice converts a bit-packed uint3 slice to an uint8 slice.
func UnpackUint3Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/3)
	n := 0
	for len(src) >= 3 {
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
		d := dst[n : n+8 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7)
		d[1] = uint8((val >> 3) & 0x7)
		d[2] = uint8((val >> 6) & 0x7)
		d[3] = uint8((val >> 9) & 0x7)
		d[4] = uint8((val >> 12) & 0x7)
		d[5] = uint8((val >> 15) & 0x7)
		d[6] = uint8((val >> 18) & 0x7)
		d[7] = uint8((val >> 21) & 0x7)
		n += 8
		src = src[3:]
	}
	switch len(src) {
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<8
		d := dst[n : n+5 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7)
		d[1] = uint8((val >> 3) & 0x7)
		d[2] = uint8((val >> 6) & 0x7)
		d[3] = uint8((val >> 9) & 0x7)
		d[4] = uint8((val >> 12) & 0x7)
		n += 5
	case 1:
		val := uint8(src[0])
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7)
		d[1] = uint8((val >> 3) & 0x7)
		n += 2
	}
	return dst
}

// UnpackUint4Slice converts a bit-packed uint4 slice to an uint8 slice.
func UnpackUint4Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/4)
	n := 0
	for len(src) >= 1 {
		val := uint8(src[0])
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 0xf)
		d[1] = uint8((val >> 4) & 0xf)
		n += 2
		src = src[1:]
	}
	return dst
}

// UnpackUint5Slice converts a bit-packed uint5 slice to an uint8 slice.
func UnpackUint5Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/5)
	n := 0
	for len(src) >= 5 {
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32
		d := dst[n : n+8 : len(dst)]
		d[0] = uint8((val >> 0) & 0x1f)
		d[1] = uint8((val >> 5) & 0x1f)
		d[2] = uint8((val >> 10) & 0x1f)
		d[3] = uint8((val >> 15) & 0x1f)
		d[4] = uint8((val >> 20) & 0x1f)
		d[5] = uint8((val >> 25) & 0x1f)
		d[6] = uint8((val >> 30) & 0x1f)
		d[7] = uint8((val >> 35) & 0x1f)
		n += 8
		src = src[5:]
	}
	switch len(src) {
	case 4:
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16 | uint32(src[3])<<24
		d := dst[n : n+6 : len(dst)]
		d[0] = uint8((val >> 0) & 0x1f)
		d[1] = uint8((val >> 5) & 0x1f)
		d[2] = uint8((val >> 10) & 0x1f)
		d[3] = uint8((val >> 15) & 0x1f)
		d[4] = uint8((val >> 20) & 0x1f)
		d[5] = uint8((val >> 25) & 0x1f)
		n += 6
	case 3:
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 0x1f)
		d[1] = uint8((val >> 5) & 0x1f)
		d[2] = uint8((val >> 10) & 0x1f)
		d[3] = uint8((val >> 15) & 0x1f)
		n += 4
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<8
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 0x1f)
		d[1] = uint8((val >> 5) & 0x1f)
		d[2] = uint8((val >> 10) & 0x1f)
		n += 3
	case 1:
		val := uint8(src[0])
		d := dst[n : n+1 : len(dst)]
		d[0] = uint8((val >> 0) & 0x1f)
		n += 1
	}
	return dst
}

// UnpackUint6Slice converts a bit-packed uint6 slice to an uint8 slice.
func UnpackUint6Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/6)
	n := 0
	for len(src) >= 3 {
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 0x3f)
		d[1] = uint8((val >> 6) & 0x3f)
		d[2] = uint8((val >> 12) & 0x3f)
		d[3] = uint8((val >> 18) & 0x3f)
		n += 4
		src = src[3:]
	}
	switch len(src) {
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<8
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 0x3f)
		d[1] = uint8((val >> 6) & 0x3f)
		n += 2
	case 1:
		val := uint8(src[0])
		d := dst[n : n+1 : len(dst)]
		d[0] = uint8((val >> 0) & 0x3f)
		n += 1
	}
	return dst
}

// UnpackUint7Slice converts a bit-packed uint7 slice to an uint8 slice.
func UnpackUint7Slice(src []byte) []uint8 {
	dst := make([]uint8, (len(src)*8)/7)
	n := 0
	for len(src) >= 7 {
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32 | uint64(src[5])<<40 | uint64(src[6])<<48
		d := dst[n : n+8 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7f)
		d[1] = uint8((val >> 7) & 0x7f)
		d[2] = uint8((val >> 14) & 0x7f)
		d[3] = uint8((val >> 21) & 0x7f)
		d[4] = uint8((val >> 28) & 0x7f)
		d[5] = uint8((val >> 35) & 0x7f)
		d[6] = uint8((val >> 42) & 0x7f)
		d[7] = uint8((val >> 49) & 0x7f)
		n += 8
		src = src[7:]
	}
	switch len(src) {
	case 6:
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32 | uint64(src[5])<<40
		d := dst[n : n+6 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7f)
		d[1] = uint8((val >> 7) & 0x7f)
		d[2] = uint8((val >> 14) & 0x7f)
		d[3] = uint8((val >> 21) & 0x7f)
		d[4] = uint8((val >> 28) & 0x7f)
		d[5] = uint8((val >> 35) & 0x7f)
		n += 6
	case 5:
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32
		d := dst[n : n+5 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7f)
		d[1] = uint8((val >> 7) & 0x7f)
		d[2] = uint8((val >> 14) & 0x7f)
		d[3] = uint8((val >> 21) & 0x7f)
		d[4] = uint8((val >> 28) & 0x7f)
		n += 5
	case 4:
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16 | uint32(src[3])<<24
		d := dst[n : n+4 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7f)
		d[1] = uint8((val >> 7) & 0x7f)
		d[2] = uint8((val >> 14) & 0x7f)
		d[3] = uint8((val >> 21) & 0x7f)
		n += 4
	case 3:
		val := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
		d := dst[n : n+3 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7f)
		d[1] = uint8((val >> 7) & 0x7f)
		d[2] = uint8((val >> 14) & 0x7f)
		n += 3
	case 2:
		val := uint16(src[0]) | uint16(src[1])<<8
		d := dst[n : n+2 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7f)
		d[1] = uint8((val >> 7) & 0x7f)
		n += 2
	case 1:
		val := uint8(src[0])
		d := dst[n : n+1 : len(dst)]
		d[0] = uint8((val >> 0) & 0x7f)
		n += 1
	}
	return dst
}