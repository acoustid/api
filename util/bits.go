package util

var multiplyDeBruijnBitPosition = [32]int{
	0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
	8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31,
}

// HighestSetBit32 returns the position of the highest set bit in x.
// This is the same as int(math.Log2(x)) but only using integer operations.
// See https://graphics.stanford.edu/~seander/bithacks.html#IntegerLogDeBruijn
func HighestSetBit32(x uint32) int {
	x |= x >> 1 // first round down to one less than a power of 2
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	return multiplyDeBruijnBitPosition[(x*0x07C4ACDD)>>27]
}
