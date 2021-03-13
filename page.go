package tinydb

import (
	"hash/fnv"
	"unsafe"
)

const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

const pageHeaderSize = unsafe.Sizeof(page{})
const branchPageElementSize = unsafe.Sizeof(branchPageElement{})
const leafPageElementSize = unsafe.Sizeof(leafPageElement{})

type pgid uint64

type page struct {
	id    pgid
	flags uint16 // different pages type
	count uint16 // pageElement counts
}

func (p *page) meta() *meta {
	return (*meta)(unsafeAdd(unsafe.Pointer(p), pageHeaderSize))
}

func (p *page) branchPageElement(index uint16) *branchPageElement {
	offset := pageHeaderSize + uintptr(index)*branchPageElementSize
	return (*branchPageElement)(unsafeAdd(unsafe.Pointer(p), offset))
}

func (p *page) leafPageElement(index uint16) *leafPageElement {
	offset := pageHeaderSize + uintptr(index)*leafPageElementSize
	return (*leafPageElement)(unsafeAdd(unsafe.Pointer(p), offset))
}

// branchPageElement represents a node on a branch page
// reference see: https://cdn.jsdelivr.net/gh/lichuang/lichuang.github.io/media/imgs/20200625-boltdb-1/branch-page-layout.png
type branchPageElement struct {
	pos   uint32 // offset from pageElement to key
	ksize uint32
	pgid  pgid // child's pgid
}

func (n *branchPageElement) key() []byte {
	return unsafeByteSlice(unsafe.Pointer(n), 0, int(n.pos), int(n.pos)+int(n.ksize))
}

// leafPageElement represents a node on a leaf page
// reference see: https://cdn.jsdelivr.net/gh/lichuang/lichuang.github.io/media/imgs/20200625-boltdb-1/leaf-page-layout.png
type leafPageElement struct {
	flags uint32 // leaf-page or sub-bucket
	pos   uint32 // offset from pageElement to key
	ksize uint32
	vsize uint32
}

func (n *leafPageElement) key() []byte {
	return unsafeByteSlice(unsafe.Pointer(n), 0, int(n.pos), int(n.pos)+int(n.ksize))
}

func (n *leafPageElement) value() []byte {
	i := int(n.ksize + n.pos)
	j := i + int(n.vsize)
	return unsafeByteSlice(unsafe.Pointer(n), 0, i, j)
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
