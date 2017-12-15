// Copyright (C) 2017  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package signal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func checkBorderInterpolate(t *testing.T, input []float64, expected []float64, b Border) {
	padding := (len(expected) - len(input)) / 2
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i], b.Interpolate(input, i-padding), fmt.Sprintf("Value at index %v does not match", i-padding))
	}
}

func TestBorder_Interpolate_Constant(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	expected := []float64{0, 0, 0, 2, 3, 5, 7, 11, 0, 0, 0}
	checkBorderInterpolate(t, input, expected, Border{Type: BorderConstant, Value: 0})
}

func TestBorder_Interpolate_Nearest(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	expected := []float64{2, 2, 2, 2, 3, 5, 7, 11, 11, 11, 11}
	checkBorderInterpolate(t, input, expected, Border{Type: BorderNearest})
}

func TestBorder_Interpolate_Wrap(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	expected := []float64{5, 7, 11, 2, 3, 5, 7, 11, 2, 3, 5}
	checkBorderInterpolate(t, input, expected, Border{Type: BorderWrap})
}

func TestBorder_Interpolate_Reflect(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	expected := []float64{5, 3, 2, 2, 3, 5, 7, 11, 11, 7, 5}
	checkBorderInterpolate(t, input, expected, Border{Type: BorderReflect})
}

func TestBorder_Interpolate_Mirror(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	expected := []float64{7, 5, 3, 2, 3, 5, 7, 11, 7, 5, 3}
	checkBorderInterpolate(t, input, expected, Border{Type: BorderMirror})
}

func TestFilter(t *testing.T) {
	src := []float64{2, 3, 5, 7, 11}
	dst := make([]float64, len(src))
	kernel := []float64{1, 1, 1}
	Convolve(src, dst, kernel, Border{BorderConstant, 0.0})

	assert.Equal(t, 5, len(dst))
	e := 1e-10
	assert.InDelta(t, 0+2+3, dst[0], e)
	assert.InDelta(t, 2+3+5, dst[1], e)
	assert.InDelta(t, 3+5+7, dst[2], e)
	assert.InDelta(t, 5+7+11, dst[3], e)
	assert.InDelta(t, 7+11+0, dst[4], e)
}

/*
func TestBoxFilter(t *testing.T) {
	src := []float64{2, 3, 5, 7, 11}
	dst := make([]float64, len(src))
	BoxFilter(src, dst, 3, Border{Type: BorderWrap})
	require.Equal(t, 5, len(dst))
	assert.InDelta(t, float64(11+2+3)/3, dst[0], 1e-6)
	assert.InDelta(t, float64(2+3+5)/3, dst[1], 1e-6)
	assert.InDelta(t, float64(3+5+7)/3, dst[2], 1e-6)
	assert.InDelta(t, float64(5+7+11)/3, dst[3], 1e-6)
	assert.InDelta(t, float64(7+11+2)/3, dst[4], 1e-6)
}
*/

func TestBoxFilter_OddWidth(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	output := make([]float64, len(input))
	BoxFilter(input, output, 5, Border{Type: BorderReflect})
	expected := []float64{3., 3.8, 5.6, 7.4, 8.2}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}

func TestBoxFilter_EvenWidth(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	output := make([]float64, len(input))
	BoxFilter(input, output, 4, Border{Type: BorderReflect})
	expected := []float64{2.5, 3., 4.25, 6.5, 8.5}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}

func TestGaussianKernel(t *testing.T) {
	output := make([]float64, 1+5*2)
	GaussianKernel(output, 1.6)
	expected := []float64{
		0.00188981, 0.01096042, 0.04301196, 0.11421021, 0.20519858,
		0.24945803, 0.20519858, 0.11421021, 0.04301196, 0.01096042,
		0.00188981,
	}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}

func TestGaussianFilter_AutoWidth(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	output := make([]float64, len(input))
	GaussianFilter(input, output, 0, 1.6, Border{Type: BorderReflect})
	expected := []float64{3.19615686, 4.01840331, 5.47145861, 7.10190763, 8.21207358}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}

func TestGaussianFilter_Width(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	output := make([]float64, len(input))
	GaussianFilter(input, output, 5, 1.6, Border{Type: BorderReflect})
	expected := []float64{2.74530856, 3.61673337, 5.38572558, 7.46201557, 8.79021692}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}

func TestGradient(t *testing.T) {
	input := []float64{2, 3, 5, 7, 11}
	output := make([]float64, len(input))
	Gradient(input, output)
	expected := []float64{1.0, 1.5, 2.0, 3.0, 4.0}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}

func TestGradient_TwoValues(t *testing.T) {
	input := []float64{2, 3}
	output := make([]float64, len(input))
	Gradient(input, output)
	expected := []float64{1.0, 1.0}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}

func TestGradient_OneValue(t *testing.T) {
	input := []float64{2}
	output := make([]float64, len(input))
	Gradient(input, output)
	expected := []float64{0.0}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}

func TestGradient_Empty(t *testing.T) {
	input := []float64{}
	output := make([]float64, len(input))
	Gradient(input, output)
	expected := []float64{}
	assert.InDeltaSlice(t, expected, output, 1e-8)
}
