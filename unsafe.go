package tinydb

import (
	"reflect"
	"unsafe"
)

// only for amb64
// maxAllocSize is the size used when creating array pointers.
// 0x7FFFFFFF -> 31bit
// 7 -> 0111
const maxAllocSize = 0x7FFFFFFF

// why -> https://groups.google.com/g/golang-nuts/c/noiQZUxqnHg
// why -> https://github.com/golang/go/issues/2188

/**
see commit id: b9c28b721ad8186bdcde91c8731ed87d65c6554d
Increase max array size to 2GB.
This commit changes the maxAllocSize from 256GB to 2GB to handle large values.
It was previously 0xFFFFFFF and I tried adding one more "F" but it caused an "array too large" error.
I played around with the value some more and found that 0x7FFFFFFF (2GB) is the highest allowed value.
This does not affect how the data is stored. It is simply used for type converting pointers to array pointers in order to utilize zero copy from the mmap.
*/

// this maxAllocSize may out-of-date, because previous Go's int is 32 bits, but now Go's int had been 64 bits
// so the theoretical maximum is 0x7FFFFFFFFFFFFFFF, but this may out of your machine physical memory

func unsafeAdd(base unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + offset)
}

func unsafeByteSlice(base unsafe.Pointer, offset uintptr, i, j int) []byte {
	// See: https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	//
	// This memory is not allocated from C, but it is unmanaged by Go's
	// garbage collector and should behave similarly, and the compiler
	// should produce similar code.  Note that this conversion allows a
	// subslice to begin after the base address, with an optional offset,
	// while the URL above does not cover this case and only slices from
	// index 0.  However, the wiki never says that the address must be to
	// the beginning of a C allocation (or even that malloc was used at
	// all), so this is believed to be correct.
	return (*[maxAllocSize]byte)(unsafeAdd(base, offset))[i:j:j]
}

// unsafeSlice modifies the data, len, and cap of a slice variable pointed to by
// the slice parameter.  This helper should be used over other direct
// manipulation of reflect.SliceHeader to prevent misuse, namely, converting
// from reflect.SliceHeader to a Go slice type.
func unsafeSlice(slice, data unsafe.Pointer, len int) {
	s := (*reflect.SliceHeader)(slice)
	s.Data = uintptr(data)
	s.Cap = len
	s.Len = len
}
