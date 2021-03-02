package tinydb

import (
	"hash/fnv"
	"testing"
	"unsafe"
)

type testMeta struct {
	version  uint32
	pageSize uint32
	pgid     pgid
}

func Test_meta_sum64(t *testing.T) {
	m := meta{}

	tm := testMeta{}
	h := fnv.New64a()
	_, _ = h.Write(((*[16]byte)(unsafe.Pointer(&tm)))[:])

	if h.Sum64() != m.sum64() {
		t.Fatal("incorrect meta checksum")
	}
}
