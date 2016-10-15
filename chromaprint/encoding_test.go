package chromaprint

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestDecodeFingerprintString_Empty(t *testing.T) {
	_, err := DecodeFingerprintString("")
	assert.Error(t, err)
}

func TestDecodeFingerprintString_Invalid(t *testing.T) {
	_, err := DecodeFingerprintString("~~!@#%$$%")
	assert.Error(t, err)
}

func TestParseFingerprint_OneItemOneBit(t *testing.T) {
	fp, err := ParseFingerprint([]byte{ 0, 0, 0, 1, 1 })
	if assert.NoError(t, err) {
		assert.Equal(t, 0, fp.Version)
		assert.Equal(t, []uint32{ 1 }, fp.Hashes)
	}
}

func TestParseFingerprint_OneItemThreeBits(t *testing.T) {
	fp, err := ParseFingerprint([]byte{ 0, 0, 0, 1, 73, 0 })
	if assert.NoError(t, err) {
		assert.Equal(t, 0, fp.Version)
		assert.Equal(t, []uint32{ 7 }, fp.Hashes)
	}
}

func TestParseFingerprint_OneItemOneBitExcept(t *testing.T) {
	fp, err := ParseFingerprint([]byte{ 0, 0, 0, 1, 7, 0 })
	if assert.NoError(t, err) {
		assert.Equal(t, 0, fp.Version)
		assert.Equal(t, []uint32{ 1<<6 }, fp.Hashes)
	}
}

func TestParseFingerprint_OneItemOneBitExcept2(t *testing.T) {
	fp, err := ParseFingerprint([]byte{ 0, 0, 0, 1, 7, 2 })
	if assert.NoError(t, err) {
		assert.Equal(t, 0, fp.Version)
		assert.Equal(t, []uint32{ 1<<8 }, fp.Hashes)
	}
}

func TestParseFingerprint_TwoItems(t *testing.T) {
	fp, err := ParseFingerprint([]byte{ 0, 0, 0, 2, 65, 0 })
	if assert.NoError(t, err) {
		assert.Equal(t, 0, fp.Version)
		assert.Equal(t, []uint32{ 1, 0 }, fp.Hashes)
	}
}

func TestParseFingerprint_TwoItemsNoChange(t *testing.T) {
	fp, err := ParseFingerprint([]byte{ 0, 0, 0, 2, 1, 0 })
	if assert.NoError(t, err) {
		assert.Equal(t, 0, fp.Version)
		assert.Equal(t, []uint32{ 1, 1 }, fp.Hashes)
	}
}

func TestParseFingerprint_Empty(t *testing.T) {
	_, err := ParseFingerprint([]byte{})
	assert.Error(t, err)
}

func TestParseFingerprint_MissingHeader(t *testing.T) {
	_, err := ParseFingerprint([]byte{ 0 })
	assert.Error(t, err)
}

func TestParseFingerprint_MissingNormalBits(t *testing.T) {
	_, err := ParseFingerprint([]byte{ 0, 255, 255, 255 })
	assert.Error(t, err)
}

func TestParseFingerprint_MissingExceptionalBits(t *testing.T) {
	_, err := ParseFingerprint([]byte{ 0, 0, 0, 1, 7 })
	assert.Error(t, err)
}

func TestParseFingerprintString(t *testing.T) {
	fp, err := ParseFingerprintString("AQAAEwkjrUmSJQpUHflR9mjSJMdZpcO_Imdw9dCO9Clu4_wQPvhCB01w6xAtXNcAp5RASgDBhDSCGGIAcwA")
	if assert.NoError(t, err) {
		assert.Equal(t, fp.Version, 1)
		assert.Equal(t, []uint32{ 0xdcfc2563, 0xdcbc2421, 0xddbc3420, 0xdd9c1530, 0xdf9c6d40, 0x4f4ce540, 0x4f0ea5c0, 0x4f0e94c1, 0x4706c4c1, 0x4716c4d3, 0x473744f2, 0x473f6472, 0x457f7572, 0x457f1563, 0x44fd2763, 0x44fd2713, 0x4cfd7753, 0x4cfd5f71, 0x45bdff71 }, fp.Hashes)
	}
}

func TestValidateFingerprintString(t *testing.T) {
	assert.False(t, ValidateFingerprintString(""))
	assert.False(t, ValidateFingerprintString("@#$"))
	assert.False(t, ValidateFingerprintString("AQAAEwkjrUmSJQpUHflR9mjSJMdZpcO"))
	assert.True(t, ValidateFingerprintString("AQAAEwkjrUmSJQpUHflR9mjSJMdZpcO_Imdw9dCO9Clu4_wQPvhCB01w6xAtXNcAp5RASgDBhDSCGGIAcwA"))
}
