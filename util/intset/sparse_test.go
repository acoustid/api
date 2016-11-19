// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package intset

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestSparseBitSet(t *testing.T) {
	set := NewSparseBitSet(0)
	set.Add(1)
	assert.Equal(t, 1, set.Len())
	require.True(t, set.Contains(1))
	require.False(t, set.Contains(0))
	require.False(t, set.Contains(2))
	set.Add(100)
	assert.Equal(t, 2, set.Len())
	require.True(t, set.Contains(100))
	require.False(t, set.Contains(101))
	set.Remove(100)
	assert.Equal(t, 1, set.Len())
	require.False(t, set.Contains(100))
	for i := 0; i < 1024; i++ {
		x := rand.Uint32()
		set.Add(x)
		require.True(t, set.Contains(x))
		set.Remove(x)
		require.False(t, set.Contains(x))
	}
}

func TestSparseBitSet_Union(t *testing.T) {
	s1 := NewSparseBitSet(0)
	s1.Add(1)
	s1.Add(2)
	s2 := NewSparseBitSet(0)
	s2.Add(3)
	s2.Add(1000)
	s2.Add(1001)
	s1.Union(s2)
	require.True(t, s1.Contains(1))
	require.True(t, s1.Contains(2))
	require.True(t, s1.Contains(3))
	require.True(t, s1.Contains(1000))
	require.True(t, s1.Contains(1001))
}

func TestSparseBitSet_ReadWrite(t *testing.T) {
	s := NewSparseBitSet(0)
	data := make([]uint32, 1024)
	for i := range data {
		x := rand.Uint32()
		s.Add(x)
		data[i] = x
	}

	var buf bytes.Buffer
	err := s.Write(&buf)
	require.NoError(t, err, "write failed")

	s2 := NewSparseBitSet(0)
	err = s2.Read(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err, "read failed")

	for i := range data {
		x := data[i]
		assert.True(t, s.Contains(x), "should contain %d, but it does not", x)
	}
}

func TestSparseBitSet_MinMax(t *testing.T) {
	s := NewSparseBitSet(0)
	s.Add(4)
	s.Add(3)
	s.Add(2999)
	s.Add(3000)
	s.Add(100)
	assert.EqualValues(t, 3, s.Min(), "wrong min value")
	assert.EqualValues(t, 3000, s.Max(), "wrong max value")
}

func BenchmarkSparseBitSet_Contains(b *testing.B) {
	s := NewSparseBitSet(0)
	for i := 0; i < 1000; i++ {
		x := rand.Uint32() & 0xffff
		s.Add(x)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Contains(rand.Uint32() & 0xffff)
	}
}

func BenchmarkSparseBitSet_Add(b *testing.B) {
	b.ReportAllocs()
	s := NewSparseBitSet(0)
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			s.Add(rand.Uint32() & 0xfffff)
		}
	}
}
