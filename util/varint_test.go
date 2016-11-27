package util

import "testing"

func TestUvarint32(t *testing.T) {
	var buf [5]byte
	for i := uint32(0); i < 1<<17; i++ {
		n := PutUvarint32(buf[:], i)
		j, n := Uvarint32(buf[:n])
		if i != j {
			t.Errorf("Uvarint32 encoding for %v was wrong, got %v after decoding", i, j)
		}
	}
}

func TestSQLiteUvarint32(t *testing.T) {
	var buf [5]byte
	for i := uint32(0); i < 1<<17; i++ {
		n := PutSQLiteUvarint32(buf[:], i)
		j, n := SQLiteUvarint32(buf[:n])
		if i != j {
			t.Errorf("SQLiteUvarint32 encoding for %v was wrong, got %v after decoding", i, j)
		}
	}
}
