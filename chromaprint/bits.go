package chromaprint

func unpackInt3Array(src []byte) []int {
	dest := make([]int, (len(src) * 8) / 3)

	si, di := 0, 0
	n := (len(src) / 3) * 3
	for si < n {
		val := uint(src[si+0]) | uint(src[si+1])<<8 | uint(src[si+2])<<16
		for i := 0; i < 8; i++ {
			dest[di+i] = int((val>>uint(3*i))&0x7)
		}
		si += 3
		di += 8
	}

	remain := len(src) - si
	switch remain {
	case 2:
		val := uint(src[si+0]) | uint(src[si+1])<<8
		for i := 0; i < 5; i++ {
			dest[di+i] = int((val>>uint(3*i))&0x7)
		}
		di += 5
	case 1:
		val := uint(src[si+0])
		for i := 0; i < 2; i++ {
			dest[di+i] = int((val>>uint(3*i))&0x7)
		}
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
		for i := 0; i < 8; i++ {
			dest[di+i] = int((val>>uint(5*i))&0x1f)
		}
		si += 5
		di += 8
	}

	remain := len(src) - si
	switch remain {
	case 4:
		val := uint(src[si+0]) | uint(src[si+1])<<8 | uint(src[si+2])<<16 | uint(src[si+3])<<24
		for i := 0; i < 6; i++ {
			dest[di+i] = int((val>>uint(5*i))&0x1f)
		}
		di += 6
	case 3:
		val := uint(src[si+0]) | uint(src[si+1])<<8 | uint(src[si+2])<<16
		for i := 0; i < 4; i++ {
			dest[di+i] = int((val>>uint(5*i))&0x1f)
		}
		di += 4
	case 2:
		val := uint(src[si+0]) | uint(src[si+1])<<8
		for i := 0; i < 3; i++ {
			dest[di+i] = int((val>>uint(5*i))&0x1f)
		}
		di += 3
	case 1:
		val := uint(src[si+0])
		for i := 0; i < 1; i++ {
			dest[di+i] = int((val>>uint(5*i))&0x1f)
		}
		di += 1
	}

	return dest
}
