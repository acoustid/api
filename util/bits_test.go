package util

import (
	"math"
	"math/rand"
	"testing"
)

func TestHighestSetBit32(t *testing.T) {
	y := HighestSetBit32(0)
	if y != 0 {
		t.Errorf("HighestOneBit32(0) returned %v, but should return 0", y)
	}
	for b := 0; b < 32; b++ {
		x := uint32(1) << uint(b)
		y := HighestSetBit32(x)
		if y != b {
			t.Errorf("HighestOneBit32(%v) returned %v, but should return %v", x, y, b)
		}
	}
	for i := 0; i < 1000; i++ {
		x := rand.Uint32()
		for x == 0 {
			x = rand.Uint32()
		}
		y1 := HighestSetBit32(x)
		y2 := int(math.Log2(float64(x)))
		if y1 != y2 {
			t.Errorf("HighestOneBit32(%v) returned %v, but should return %v", x, y1, y2)
		}
	}
}
