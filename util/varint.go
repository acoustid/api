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
