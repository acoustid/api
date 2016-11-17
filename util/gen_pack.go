// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

// +build ignore

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func genUnpackIntArrayInner(bits int, sblock int, lines []string, pack bool) []string {
	dblock := sblock * 8 / bits

	var vtype string
	switch {
	case sblock <= 1:
		vtype = "uint8"
	case sblock <= 2:
		vtype = "uint16"
	case sblock <= 4:
		vtype = "uint32"
	default:
		vtype = "uint64"
	}

	src := make([]string, sblock)
	for i := range src {
		s := fmt.Sprintf("%s(src[%d])", vtype, i)
		if i > 0 {
			s += fmt.Sprintf("<<%d", i*8)
		}
		src[i] = s
	}
	if pack {
		lines = append(lines, fmt.Sprintf("\t\tval := %s", strings.Join(src, " | ")))
	}
	lines = append(lines, fmt.Sprintf("\t\td := dst[n : n+%d : len(dst)]", dblock))
	for i := 0; i < dblock; i++ {
		lines = append(lines, fmt.Sprintf("\t\td[%d] = uint8((val >> %d) & %d)", i, bits*i, (1<<uint(bits))-1))
	}
	lines = append(lines, fmt.Sprintf("\t\tn += %d", dblock))
	return lines
}

func genUnpackIntArray(bits int) string {
	var sblock int
	for i := 1; i <= bits; i++ {
		if (i*8)%bits == 0 {
			sblock = i
			break
		}
	}
	var lines []string
	lines = append(lines, fmt.Sprintf("// UnpackUint%dSlice converts a bit-packed uint%d slice to an uint8 slice.", bits, bits))
	lines = append(lines, fmt.Sprintf("func UnpackUint%dSlice(src []byte) []uint8 {", bits))
	lines = append(lines, fmt.Sprintf("\tdst := make([]uint8, (len(src)*8)/%d)", bits))
	lines = append(lines, fmt.Sprintf("\tn := 0"))
	if sblock == 1 {
		lines = append(lines, fmt.Sprintf("\tfor _, val := range src {"))
		lines = genUnpackIntArrayInner(bits, sblock, lines, false)
	} else {
		lines = append(lines, fmt.Sprintf("\tfor len(src) >= %d {", sblock))
		lines = genUnpackIntArrayInner(bits, sblock, lines, true)
		lines = append(lines, fmt.Sprintf("\t\tsrc = src[%d:]", sblock))
	}
	lines = append(lines, "\t}")
	if sblock > 1 {
		lines = append(lines, fmt.Sprintf("\tswitch len(src) {"))
		for i := sblock - 1; i > 0; i-- {
			lines = append(lines, fmt.Sprintf("\tcase %d:", i))
			lines = genUnpackIntArrayInner(bits, i, lines, true)
		}
		lines = append(lines, "\t}")
	}
	lines = append(lines, "\treturn dst", "}")
	return strings.Join(lines, "\n")
}

func genPackIntArrayInner(bits int, dblock int, lines []string, pack bool) []string {
	sblock := (dblock*bits + 7) / 8

	var vtype string
	switch {
	case sblock <= 1:
		vtype = "uint8"
	case sblock <= 2:
		vtype = "uint16"
	case sblock <= 4:
		vtype = "uint32"
	default:
		vtype = "uint64"
	}

	src := make([]string, dblock)
	for i := range src {
		s := fmt.Sprintf("%s(src[%d])", vtype, i)
		if i > 0 {
			s += fmt.Sprintf("<<%d", i*bits)
		}
		src[i] = s
	}
	if pack {
		lines = append(lines, fmt.Sprintf("\t\tval := %s", strings.Join(src, " | ")))
	}
	if sblock == 1 {
		lines = append(lines, fmt.Sprintf("\t\tdst[n] = uint8(val & 255)"))
	} else {
		lines = append(lines, fmt.Sprintf("\t\td := dst[n : n+%d : len(dst)]", sblock))
		for i := 0; i < sblock; i++ {
			lines = append(lines, fmt.Sprintf("\t\td[%d] = uint8((val >> %d) & 255)", i, 8*i))
		}
	}
	lines = append(lines, fmt.Sprintf("\t\tn += %d", sblock))
	return lines
}

func genPackIntArray(bits int) string {
	var sblock int
	for i := 1; i <= bits; i++ {
		if (i*8)%bits == 0 {
			sblock = i
			break
		}
	}
	dblock := sblock * 8 / bits
	var lines []string
	lines = append(lines, fmt.Sprintf("// PackUint%dSlice converts an uint8 slice into a bit-packed uint%d slice.", bits, bits))
	lines = append(lines, fmt.Sprintf("func PackUint%dSlice(dst []byte, src []uint8) int {", bits))
	lines = append(lines, fmt.Sprintf("\tn := 0"))
	if dblock == 1 {
		lines = append(lines, fmt.Sprintf("\tfor _, val := range src {"))
		lines = genPackIntArrayInner(bits, dblock, lines, false)
	} else {
		lines = append(lines, fmt.Sprintf("\tfor len(src) >= %d {", dblock))
		lines = genPackIntArrayInner(bits, dblock, lines, true)
		lines = append(lines, fmt.Sprintf("\t\tsrc = src[%d:]", dblock))
	}
	lines = append(lines, "\t}")
	if dblock > 1 {
		lines = append(lines, fmt.Sprintf("\tswitch len(src) {"))
		for i := dblock - 1; i > 0; i-- {
			lines = append(lines, fmt.Sprintf("\tcase %d:", i))
			lines = genPackIntArrayInner(bits, i, lines, true)
		}
		lines = append(lines, "\t}")
	}
	lines = append(lines, "\treturn n", "}")
	return strings.Join(lines, "\n")
}

func main() {
	file, err := os.Create("pack.go")
	if err != nil {
		log.Fatalf("failed to create output file: %v", err)
	}
	defer file.Close()
	sections := []string{
		"// Copyright (C) 2016  Lukas Lalinsky\n" +
			"// Distributed under the MIT license, see the LICENSE file for details.\n\n" +
			"// THIS FILE WAS AUTOMATICALLY GENERATED, DO NOT EDIT\n\n" +
			"package util",
	}
	for i := 1; i < 8; i++ {
		sections = append(sections, genPackIntArray(i))
		sections = append(sections, genUnpackIntArray(i))
	}
	io.WriteString(file, strings.Join(sections, "\n\n"))
}
