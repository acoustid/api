package chromaprint

func unpackInt3Array(src []byte) []int8 {
	dest := make([]int8, (len(src)*8)/3)

	di := 0
	for len(src) >= 6 {
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32 | uint64(src[5])<<40
		d := dest[di : di+16 : len(dest)]
		d[0] = int8((val >> (3 * 0)) & 0x7)
		d[1] = int8((val >> (3 * 1)) & 0x7)
		d[2] = int8((val >> (3 * 2)) & 0x7)
		d[3] = int8((val >> (3 * 3)) & 0x7)
		d[4] = int8((val >> (3 * 4)) & 0x7)
		d[5] = int8((val >> (3 * 5)) & 0x7)
		d[6] = int8((val >> (3 * 6)) & 0x7)
		d[7] = int8((val >> (3 * 7)) & 0x7)
		d[8] = int8((val >> (3 * 8)) & 0x7)
		d[9] = int8((val >> (3 * 9)) & 0x7)
		d[10] = int8((val >> (3 * 10)) & 0x7)
		d[11] = int8((val >> (3 * 11)) & 0x7)
		d[12] = int8((val >> (3 * 12)) & 0x7)
		d[13] = int8((val >> (3 * 13)) & 0x7)
		d[14] = int8((val >> (3 * 14)) & 0x7)
		d[15] = int8((val >> (3 * 15)) & 0x7)
		di += 16
		src = src[6:]
	}

	for len(src) >= 3 {
		val := uint(src[0]) | uint(src[1])<<8 | uint(src[2])<<16
		d := dest[di : di+8 : len(dest)]
		d[0] = int8((val >> (3 * 0)) & 0x7)
		d[1] = int8((val >> (3 * 1)) & 0x7)
		d[2] = int8((val >> (3 * 2)) & 0x7)
		d[3] = int8((val >> (3 * 3)) & 0x7)
		d[4] = int8((val >> (3 * 4)) & 0x7)
		d[5] = int8((val >> (3 * 5)) & 0x7)
		d[6] = int8((val >> (3 * 6)) & 0x7)
		d[7] = int8((val >> (3 * 7)) & 0x7)
		di += 8
		src = src[3:]
	}

	switch len(src) {
	case 2:
		val := uint(src[0]) | uint(src[1])<<8
		d := dest[di : di+5 : len(dest)]
		d[0] = int8((val >> (3 * 0)) & 0x7)
		d[1] = int8((val >> (3 * 1)) & 0x7)
		d[2] = int8((val >> (3 * 2)) & 0x7)
		d[3] = int8((val >> (3 * 3)) & 0x7)
		d[4] = int8((val >> (3 * 4)) & 0x7)
	case 1:
		val := uint(src[0])
		d := dest[di : di+2 : len(dest)]
		d[0] = int8((val >> (3 * 0)) & 0x7)
		d[1] = int8((val >> (3 * 1)) & 0x7)
	}

	return dest
}

func unpackInt5Array(src []byte) []int8 {
	dest := make([]int8, (len(src)*8)/5)

	di := 0
	for len(src) >= 5 {
		val := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32
		d := dest[di : di+8 : len(dest)]
		d[0] = int8((val >> (5 * 0)) & 0x1f)
		d[1] = int8((val >> (5 * 1)) & 0x1f)
		d[2] = int8((val >> (5 * 2)) & 0x1f)
		d[3] = int8((val >> (5 * 3)) & 0x1f)
		d[4] = int8((val >> (5 * 4)) & 0x1f)
		d[5] = int8((val >> (5 * 5)) & 0x1f)
		d[6] = int8((val >> (5 * 6)) & 0x1f)
		d[7] = int8((val >> (5 * 7)) & 0x1f)
		di += 8
		src = src[5:]
	}

	switch len(src) {
	case 4:
		val := uint(src[0]) | uint(src[1])<<8 | uint(src[2])<<16 | uint(src[3])<<24
		d := dest[di : di+6 : len(dest)]
		d[0] = int8((val >> (5 * 0)) & 0x1f)
		d[1] = int8((val >> (5 * 1)) & 0x1f)
		d[2] = int8((val >> (5 * 2)) & 0x1f)
		d[3] = int8((val >> (5 * 3)) & 0x1f)
		d[4] = int8((val >> (5 * 4)) & 0x1f)
		d[5] = int8((val >> (5 * 5)) & 0x1f)
	case 3:
		val := uint(src[0]) | uint(src[1])<<8 | uint(src[2])<<16
		d := dest[di : di+4 : len(dest)]
		d[0] = int8((val >> (5 * 0)) & 0x1f)
		d[1] = int8((val >> (5 * 1)) & 0x1f)
		d[2] = int8((val >> (5 * 2)) & 0x1f)
		d[3] = int8((val >> (5 * 3)) & 0x1f)
	case 2:
		val := uint(src[0]) | uint(src[1])<<8
		d := dest[di : di+3 : len(dest)]
		d[0] = int8((val >> (5 * 0)) & 0x1f)
		d[1] = int8((val >> (5 * 1)) & 0x1f)
		d[2] = int8((val >> (5 * 2)) & 0x1f)
	case 1:
		val := uint(src[0])
		d := dest[di : di+1 : len(dest)]
		d[0] = int8((val >> (5 * 0)) & 0x1f)
	}

	return dest
}
