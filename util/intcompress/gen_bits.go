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

func main() {
	file, err := os.Create("bits.go")
	if err != nil {
		log.Fatalf("failed to create output file: %v", err)
	}
	defer file.Close()
	sections := []string{"package intcompress"}
	for i := 1; i < 8; i++ {
		sections = append(sections, genUnpackIntArray(i))
	}
	io.WriteString(file, strings.Join(sections, "\n\n"))
}
