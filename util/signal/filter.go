// Copyright (C) 2017  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package signal

import "math"

type BorderType int

const (
	BorderConstant BorderType = iota
	BorderNearest
	BorderWrap
	BorderReflect
	BorderMirror
)

type Border struct {
	Type  BorderType
	Value float64
}

func (b Border) Interpolate(src []float64, idx int) float64 {
	if idx >= 0 && idx < len(src) {
		return src[idx]
	}
	n := len(src)
	switch b.Type {
	case BorderConstant:
		return b.Value
	case BorderNearest:
		if idx < 0 {
			return src[0]
		} else {
			return src[n-1]
		}
	case BorderWrap:
		if idx < 0 {
			return src[((idx%n)+n)%n]
		} else {
			return src[idx%n]

		}
	case BorderMirror:
		if n == 1 {
			return src[0]
		}
		for idx < 0 || idx >= n {
			if idx < 0 {
				idx = -idx
			} else {
				idx = 2*n - idx - 2
			}
		}
		return src[idx]
	case BorderReflect:
		if n == 1 {
			return src[0]
		}
		for idx < 0 || idx >= n {
			if idx < 0 {
				idx = -idx - 1
			} else {
				idx = 2*n - idx - 1
			}
		}
		return src[idx]
	}
	return 0
}

// Convolve convolves the input with an arbitrary kernel.
func Convolve(src, dst []float64, kernel []float64, border Border) {
	width := len(kernel)
	halfWidth := width / 2
	for i := range dst {
		k := i - halfWidth
		var sum float64
		if k >= 0 && k <= len(src)-width {
			for j, x := range kernel {
				sum += src[k+j] * x
			}
		} else {
			for j, x := range kernel {
				sum += border.Interpolate(src, k+j) * x
			}
		}
		dst[i] = sum
	}
}

// BoxFilter convolves the input with a uniform kernel.
func BoxFilter(src, dst []float64, width int, border Border) {
	// TODO optimize
	alpha := 1.0 / float64(width)
	kernel := make([]float64, width)
	for i := range kernel {
		kernel[i] = alpha
	}
	Convolve(src, dst, kernel, border)
}

// GaussianKernel computes a 1D Gaussian convolution kernel and writes the result to dst.
// The size of the kernel is determined by the length of dst. It must be an odd number.
// The sigma parameter represents the standard deviation of the Gaussian.
func GaussianKernel(dst []float64, sigma float64) {
	var sum float64
	scale := -1.0 / (2 * sigma * sigma)
	for i := range dst {
		x := float64(i - (len(dst)-1)/2)
		t := math.Exp(x * x * scale)
		dst[i] = t
		sum += t
	}
	alpha := 1 / sum
	for i := range dst {
		dst[i] *= alpha
	}
}

// GaussianFilter convolves the input with a Gaussian kernel.
// If the width is not a positive number, it will be automatically calculated as 4 standard deviations of the Gaussian.
// The sigma parameter represents the standard deviation of the Gaussian.
func GaussianFilter(src, dst []float64, width int, sigma float64, border Border) {
	if width <= 0 {
		width = 1 + 2*int(math.Floor(sigma*4+0.5))
	}
	kernel := make([]float64, width)
	GaussianKernel(kernel, sigma)
	Convolve(src, dst, kernel, border)
}

// Gradient calculates the gradient the input array.
// The gradient is computed using second order accurate central differences in the interior points
// and first order accurate differences at the boundaries. The returned gradient hence has
// the same shape as the input array.
func Gradient(src, dst []float64) {
	n := len(src)
	if n == 0 {
		return
	}
	if n == 1 {
		dst[0] = 0
		return
	}
	dst[0] = src[1] - src[0]
	for i := 1; i < n-1; i++ {
		dst[i] = 0.5 * (src[i+1] - src[i-1])
	}
	dst[n-1] = src[n-1] - src[n-2]
}
