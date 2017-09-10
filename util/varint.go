package util

const (
	MaxVarintLen32 = 5
)

// PutUvarint32 encodes a uint32 into buf and returns the number of bytes written.
// If the buffer is too small, PutUvarint32 will panic.
func PutUvarint32(buf []byte, x uint32) int {
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	return i + 1
}

// Uvarint32 decodes a uint32 from buf and returns that value and the
// number of bytes read (> 0). If an error occurred, the value is 0
// and the number of bytes n is <= 0 meaning:
//
//	n == 0: buf too small
//	n  < 0: value larger than 32 bits (overflow)
//              and -n is the number of bytes read
//
func Uvarint32(buf []byte) (uint32, int) {
	var x uint32
	var s uint
	for i, b := range buf {
		if b < 0x80 {
			if i > 4 || i == 4 && b > 15 {
				return 0, -(i + 1) // overflow
			}
			return x | uint32(b)<<s, i + 1
		}
		x |= uint32(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

// http://www.sqlite.org/src4/doc/trunk/www/varint.wiki

func PutSQLiteUvarint32(buf []byte, x uint32) int {
	if x < 245 {
		buf[0] = byte(x)
		return 1
	}
	if x < 2292 {
		buf[0] = byte((x-244)/256 + 245)
		buf[1] = byte((x - 244) % 256)
		return 2
	}
	if x < 67828 {
		buf[0] = 253
		buf[1] = byte((x - 2292) / 256)
		buf[2] = byte((x - 2292) % 256)
		return 3
	}
	if x < 1<<24 {
		buf[0] = 254
		buf[1] = byte(x >> 16)
		buf[2] = byte(x >> 8)
		buf[3] = byte(x)
		return 4
	}
	buf[0] = 255
	buf[1] = byte(x >> 24)
	buf[2] = byte(x >> 16)
	buf[3] = byte(x >> 8)
	buf[4] = byte(x)
	return 5
}

func SQLiteUvarint32(buf []byte) (uint32, int) {
	if buf[0] <= 244 {
		return uint32(buf[0]), 1
	}
	if buf[0] <= 252 {
		return 244 + 256*(uint32(buf[0])-245) + uint32(buf[1]), 2
	}
	if buf[0] == 253 {
		return 2292 + 256*uint32(buf[1]) + uint32(buf[2]), 3
	}
	if buf[0] == 254 {
		return uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3]), 4
	}
	return uint32(buf[1])<<24 | uint32(buf[2])<<16 | uint32(buf[3])<<8 | uint32(buf[4]), 5
}
