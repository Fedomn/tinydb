package tinydb

import (
	"hash/fnv"
	"unsafe"
)

type pgid uint64

type page struct {
	id pgid
}

func (p *page) meta() *meta {
	return (*meta)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
}

type meta struct {
	version  uint32
	pageSize uint32
	pgid     pgid
	checksum uint64
}

func (m *meta) sum64() uint64 {
	h := fnv.New64a()
	// data struct memory alignment
	// unsafe.Offsetof(meta{}.checksum) = version + pageSize + pgid = uint32+uint32+uint64 = 16byte
	// (*[16]byte)(unsafe.Pointer(m)) -> force to take needed fields before checksum
	dataBeforeChecksum := (*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))
	_, _ = h.Write(dataBeforeChecksum[:])
	return h.Sum64()
}

func (m *meta) validate() error {
	if m.version != tinyDBVersion {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}
