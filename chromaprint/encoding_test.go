// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package chromaprint

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	TestFingerprintString  = "AQAAEwkjrUmSJQpUHflR9mjSJMdZpcO_Imdw9dCO9Clu4_wQPvhCB01w6xAtXNcAp5RASgDBhDSCGGIAcwA"
	TestFingerprintData    = []byte{0x1, 0x0, 0x0, 0x13, 0x9, 0x23, 0xad, 0x49, 0x92, 0x25, 0xa, 0x54, 0x1d, 0xf9, 0x51, 0xf6, 0x68, 0xd2, 0x24, 0xc7, 0x59, 0xa5, 0xc3, 0xbf, 0x22, 0x67, 0x70, 0xf5, 0xd0, 0x8e, 0xf4, 0x29, 0x6e, 0xe3, 0xfc, 0x10, 0x3e, 0xf8, 0x42, 0x7, 0x4d, 0x70, 0xeb, 0x10, 0x2d, 0x5c, 0xd7, 0x0, 0xa7, 0x94, 0x40, 0x4a, 0x0, 0xc1, 0x84, 0x34, 0x82, 0x18, 0x62, 0x0, 0x73, 0x0}
	TestFingerprintVersion = 1
	TestFingerprintHashes  = []uint32{0xdcfc2563, 0xdcbc2421, 0xddbc3420, 0xdd9c1530, 0xdf9c6d40, 0x4f4ce540, 0x4f0ea5c0, 0x4f0e94c1, 0x4706c4c1, 0x4716c4d3, 0x473744f2, 0x473f6472, 0x457f7572, 0x457f1563, 0x44fd2763, 0x44fd2713, 0x4cfd7753, 0x4cfd5f71, 0x45bdff71}
)

var (
	TestFingerprint2String  = "AQAAI0kSJUsURVESwcTxQ8fxQz9-lNJDhD8cSTxagj9KibGwbRVa4jxOwirjANdxXA96HW-RHnppFKeE_vhxStAJixQATAiIxAkGGQKOEQkgQZYrY4gQwAjGkBAA"
	TestFingerprint2Data    = []byte{0x1, 0x0, 0x0, 0x23, 0x49, 0x12, 0x25, 0x4b, 0x14, 0x45, 0x51, 0x12, 0xc1, 0xc4, 0xf1, 0x43, 0xc7, 0xf1, 0x43, 0x3f, 0x7e, 0x94, 0xd2, 0x43, 0x84, 0x3f, 0x1c, 0x49, 0x3c, 0x5a, 0x82, 0x3f, 0x4a, 0x89, 0xb1, 0xb0, 0x6d, 0x15, 0x5a, 0xe2, 0x3c, 0x4e, 0xc2, 0x2a, 0xe3, 0x0, 0xd7, 0x71, 0x5c, 0xf, 0x7a, 0x1d, 0x6f, 0x91, 0x1e, 0x7a, 0x69, 0x14, 0xa7, 0x84, 0xfe, 0xf8, 0x71, 0x4a, 0xd0, 0x9, 0x8b, 0x14, 0x0, 0x4c, 0x8, 0x88, 0xc4, 0x9, 0x6, 0x19, 0x2, 0x8e, 0x11, 0x9, 0x20, 0x41, 0x96, 0x2b, 0x63, 0x88, 0x10, 0xc0, 0x8, 0xc6, 0x90, 0x10, 0x0}
	TestFingerprint2Version = 1
	TestFingerprint2Hashes  = []uint32{795720159, 795720703, 795458559, 795589375, 778812157, 778828541, 774634493, 778828287, 644594175, 938192111, 904635646, 367702430, 367694734, 99255174, 124360342, 124559282, 124566946, 91011554, 91072738, 82753602, 82753602, 1156492354, 1157540930, 1165919298, 1199555666, 1199428818, 1199437043, 1207948785, 1207948769, 1207793633, 1174235121, 1165850609, 1165877105, 1165884787, 1165981139}
)

func TestDecodeFingerprintString(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		_, err := DecodeFingerprintString("")
		assert.Error(t, err, "decoding an empty string should fail")
	})
	t.Run("InvalidChars", func(t *testing.T) {
		_, err := DecodeFingerprintString("~~!@#%$$%")
		assert.Error(t, err, "decoding a string with characters outside of base64 should fail")
	})
	t.Run("Valid", func(t *testing.T) {
		_, err := DecodeFingerprintString("~~!@#%$$%")
		data, err := DecodeFingerprintString(TestFingerprintString)
		if assert.NoError(t, err, "failed to decode a valid fingerprint string") {
			assert.Equal(t, TestFingerprintData, data, "decoded fingerprint data does not match")
		}
	})
}

func TestEncodeFingerprintToString(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		_, err := DecodeFingerprintString("")
		assert.Error(t, err, "decoding an empty string should fail")
	})
	t.Run("Valid", func(t *testing.T) {
		str := EncodeFingerprintToString(TestFingerprintData)
		assert.Equal(t, TestFingerprintString, str, "encoded fingerprint strings does not match")
	})
}

func TestParseFingerprint(t *testing.T) {
	cases := []struct {
		name   string
		in     []byte
		valid  bool
		fp     Fingerprint
		reason string
	}{
		{name: "OneItemOneBit", in: []byte{0, 0, 0, 1, 1}, valid: true, fp: Fingerprint{Version: 0, Hashes: []uint32{1}}},
		{name: "OneItemThreeBits", in: []byte{0, 0, 0, 1, 73, 0}, valid: true, fp: Fingerprint{Version: 0, Hashes: []uint32{7}}},
		{name: "OneItemOneBitExcept", in: []byte{0, 0, 0, 1, 7, 0}, valid: true, fp: Fingerprint{Version: 0, Hashes: []uint32{1 << 6}}},
		{name: "OneItemOneBitExcept2", in: []byte{0, 0, 0, 1, 7, 2}, valid: true, fp: Fingerprint{Version: 0, Hashes: []uint32{1 << 8}}},
		{name: "TwoItems", in: []byte{0, 0, 0, 2, 65, 0}, valid: true, fp: Fingerprint{Version: 0, Hashes: []uint32{1, 0}}},
		{name: "TwoItemsNoChange", in: []byte{0, 0, 0, 2, 1, 0}, valid: true, fp: Fingerprint{Version: 0, Hashes: []uint32{1, 1}}},
		{name: "Long", in: TestFingerprintData, valid: true, fp: Fingerprint{Version: TestFingerprintVersion, Hashes: TestFingerprintHashes}},
		{name: "Long2", in: TestFingerprint2Data, valid: true, fp: Fingerprint{Version: TestFingerprint2Version, Hashes: TestFingerprint2Hashes}},
		{name: "Empty", in: []byte{}, valid: false},
		{name: "MissingHeader", in: []byte{0}, valid: false},
		{name: "MissingNormalBits", in: []byte{0, 255, 255, 255}, valid: false},
		{name: "MissingExceptionalBits", in: []byte{0, 0, 0, 1, 7}, valid: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fp, err := ParseFingerprint(c.in)
			if c.valid {
				if assert.NoError(t, err, "failed to decode fingerprint") {
					assert.Equal(t, c.fp.Version, fp.Version, "decoded fingerprint version does not match")
					assert.Equal(t, c.fp.Hashes, fp.Hashes, "decoded fingerprint hashes do not match")
				}
				d := CompressFingerprint(c.fp)
				assert.Equal(t, c.in, d)
			} else {
				assert.Error(t, err, "should not successfully decode invalid fingerprint data")
			}
		})
	}
}

func TestParseFingerprintString(t *testing.T) {
	fp, err := ParseFingerprintString(TestFingerprintString)
	if assert.NoError(t, err) {
		assert.Equal(t, TestFingerprintVersion, fp.Version)
		assert.Equal(t, TestFingerprintHashes, fp.Hashes)
	}
}

func TestParseFingerprintString2(t *testing.T) {
	fp, err := ParseFingerprintString(TestFingerprint2String)
	if assert.NoError(t, err) {
		assert.Equal(t, TestFingerprint2Version, fp.Version)
		assert.Equal(t, TestFingerprint2Hashes, fp.Hashes)
	}
}

func TestValidateFingerprintString(t *testing.T) {
	assert.False(t, ValidateFingerprintString(""))
	assert.False(t, ValidateFingerprintString("@#$"))
	assert.False(t, ValidateFingerprintString("AQAAEwkjrUmSJQpUHflR9mjSJMdZpcO"))
	assert.True(t, ValidateFingerprintString(TestFingerprintString))
	assert.True(t, ValidateFingerprintString(TestFingerprint2String))
}

func ExampleDecodeFingerprintString() {
	input := "AQAAA5IULYmZJCgcNwcC"
	bytes, err := DecodeFingerprintString(input)
	if err == nil {
		fmt.Println(bytes)
	}
	// Output: [1 0 0 3 146 20 45 137 153 36 40 28 55 7 2]
}

func ExampleEncodeFingerprintToString() {
	bytes := []byte{1, 0, 0, 3, 146, 20, 45, 137, 153, 36, 40, 28, 55, 7, 2}
	fmt.Println(EncodeFingerprintToString(bytes))
	// Output: AQAAA5IULYmZJCgcNwcC
}

func ExampleValidateFingerprint() {
	input := []byte{1, 0, 0, 3, 146, 20, 45, 137, 153, 36, 40, 28, 55, 7, 2}
	fmt.Println(ValidateFingerprint(input))
	// Output: true
}

func ExampleValidateFingerprintString() {
	input := "AQAAA5IULYmZJCgcNwcC"
	fmt.Println(ValidateFingerprintString(input))
	// Output: true
}

func ExampleParseFingerprint() {
	input := []byte{1, 0, 0, 3, 146, 20, 45, 137, 153, 36, 40, 28, 55, 7, 2}
	fp, err := ParseFingerprint(input)
	if err == nil {
		fmt.Println(fp.Version)
		fmt.Println(fp.Hashes)
	}
	// Output:
	// 1
	// [2084693418 2084693434 1950873050]
}

func ExampleParseFingerprintString() {
	input := "AQAAA5IULYmZJCgcNwcC"
	fp, err := ParseFingerprintString(input)
	if err == nil {
		fmt.Println(fp.Version)
		fmt.Println(fp.Hashes)
	}
	// Output:
	// 1
	// [2084693418 2084693434 1950873050]
}
