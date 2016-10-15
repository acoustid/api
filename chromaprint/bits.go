package chromaprint

func unpackInt3Array(src []byte) []int {
	dest := make([]int, (len(src) * 8) / 3)

	si, di := 0, 0
	n := (len(src) / 3) * 3
	for si < n {
		val := uint(src[si+0]) | uint(src[si+1])<<8 | uint(src[si+2])<<16
		dest[di+0] = int((val>>(3*0))&0x7)
		dest[di+1] = int((val>>(3*1))&0x7)
		dest[di+2] = int((val>>(3*2))&0x7)
		dest[di+3] = int((val>>(3*3))&0x7)
		dest[di+4] = int((val>>(3*4))&0x7)
		dest[di+5] = int((val>>(3*5))&0x7)
		dest[di+6] = int((val>>(3*6))&0x7)
		dest[di+7] = int((val>>(3*7))&0x7)
		si += 3
		di += 8
	}

	remain := len(src) - si
	switch remain {
	case 2:
		val := uint(src[si+0]) | uint(src[si+1])<<8
		dest[di+0] = int((val>>(3*0))&0x7)
		dest[di+1] = int((val>>(3*1))&0x7)
		dest[di+2] = int((val>>(3*2))&0x7)
		dest[di+3] = int((val>>(3*3))&0x7)
		dest[di+4] = int((val>>(3*4))&0x7)
		di += 5
	case 1:
		val := uint(src[si+0])
		dest[di+0] = int((val>>(3*0))&0x7)
		dest[di+1] = int((val>>(3*1))&0x7)
		di += 2
	}

	return dest
}

func unpackInt5Array(src []byte) []int {
	dest := make([]int, (len(src) * 8) / 5)

	si, di := 0, 0
	n := (len(src) / 5) * 5
	for si < n {
		val := uint64(src[si+0]) | uint64(src[si+1])<<8 | uint64(src[si+2])<<16 | uint64(src[si+3])<<24 | uint64(src[si+4])<<32
		dest[di+0] = int((val>>(5*0))&0x1f)
		dest[di+1] = int((val>>(5*1))&0x1f)
		dest[di+2] = int((val>>(5*2))&0x1f)
		dest[di+3] = int((val>>(5*3))&0x1f)
		dest[di+4] = int((val>>(5*4))&0x1f)
		dest[di+5] = int((val>>(5*5))&0x1f)
		dest[di+6] = int((val>>(5*6))&0x1f)
		dest[di+7] = int((val>>(5*7))&0x1f)
		si += 5
		di += 8
	}

	remain := len(src) - si
	switch remain {
	case 4:
		val := uint(src[si+0]) | uint(src[si+1])<<8 | uint(src[si+2])<<16 | uint(src[si+3])<<24
		dest[di+0] = int((val>>(5*0))&0x1f)
		dest[di+1] = int((val>>(5*1))&0x1f)
		dest[di+2] = int((val>>(5*2))&0x1f)
		dest[di+3] = int((val>>(5*3))&0x1f)
		dest[di+4] = int((val>>(5*4))&0x1f)
		dest[di+5] = int((val>>(5*5))&0x1f)
		di += 6
	case 3:
		val := uint(src[si+0]) | uint(src[si+1])<<8 | uint(src[si+2])<<16
		dest[di+0] = int((val>>(5*0))&0x1f)
		dest[di+1] = int((val>>(5*1))&0x1f)
		dest[di+2] = int((val>>(5*2))&0x1f)
		dest[di+3] = int((val>>(5*3))&0x1f)
		di += 4
	case 2:
		val := uint(src[si+0]) | uint(src[si+1])<<8
		dest[di+0] = int((val>>(5*0))&0x1f)
		dest[di+1] = int((val>>(5*1))&0x1f)
		dest[di+2] = int((val>>(5*2))&0x1f)
		di += 3
	case 1:
		val := uint(src[si+0])
		dest[di+0] = int((val>>(5*0))&0x1f)
		di += 1
	}

	return dest
}
