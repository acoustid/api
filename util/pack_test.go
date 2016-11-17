// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package util

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/big"
	"math/rand"
	"testing"
)

func TestPackUnpack(t *testing.T) {
	cases := []struct {
		bits     uint
		unpackFn func([]byte) []uint8
		packFn   func([]byte, []uint8) int
	}{
		{bits: 1, unpackFn: UnpackUint1Slice, packFn: PackUint1Slice},
		{bits: 2, unpackFn: UnpackUint2Slice, packFn: PackUint2Slice},
		{bits: 3, unpackFn: UnpackUint3Slice, packFn: PackUint3Slice},
		{bits: 4, unpackFn: UnpackUint4Slice, packFn: PackUint4Slice},
		{bits: 5, unpackFn: UnpackUint5Slice, packFn: PackUint5Slice},
		{bits: 6, unpackFn: UnpackUint6Slice, packFn: PackUint6Slice},
		{bits: 7, unpackFn: UnpackUint7Slice, packFn: PackUint7Slice},
	}
	for _, c := range cases {
		values := make([]uint8, 256)
		for i := range values {
			values[i] = uint8((i + 1) & (1<<c.bits - 1))
		}

		pack := func(i int) []byte {
			packed := make([]byte, ((uint(i)*c.bits)+7)/8)
			x := big.NewInt(0)
			for i := range values[:i] {
				y := big.NewInt(int64(values[i]))
				y.Lsh(y, uint(i)*c.bits)
				x.Or(x, y)
			}
			for j := range packed {
				y := big.NewInt(255)
				y.And(y, x)
				packed[j] = byte(y.Uint64())
				x.Rsh(x, 8)
			}
			return packed
		}

		t.Run(fmt.Sprintf("Bits=%d,Unpack", c.bits), func(t *testing.T) {
			for i := range values {
				expected := values[:i]
				result := c.unpackFn(pack(i))
				require.Equal(t, expected, result[:i])
				for j := len(expected); j < len(result); j++ {
					require.Zero(t, result[j])
				}
			}
		})

		t.Run(fmt.Sprintf("Bits=%d,Pack", c.bits), func(t *testing.T) {
			for i := range values {
				expected := pack(i)
				buf := make([]byte, len(values))
				n := c.packFn(buf[:], values[:i])
				require.Equal(t, expected, buf[:n])
			}
		})
	}
}

func BenchmarkUnpackInt3Array(b *testing.B) {
	r := rand.New(rand.NewSource(1234))
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(r.Uint32() & 0xff)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		UnpackUint3Slice(data)
	}
}

func BenchmarkUnpackInt5Array(b *testing.B) {
	r := rand.New(rand.NewSource(1234))
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(r.Uint32() & 0xff)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		UnpackUint5Slice(data)
	}
}
