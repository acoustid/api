package chromaprint

import (
	"testing"
)

func TestFingerprintFile(t *testing.T) {
	t.Skip("nothing to test yet")
	fp, err := FingerprintFile("/home/lukas/code/acoustid/chromaprint-test-cases/rock_is_dead_1.opus", 10)
	if err != nil {
		t.Fatalf("FingerprintFile failed: %v", err)
	}
	t.Logf("fp %v", fp)
}
