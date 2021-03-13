package tinydb

import (
	"bytes"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
type node struct {
	isLeaf   bool
	parent   *node
	children nodes
	inodes   inodes
}

type nodes []*node

// inode represents an internal node inside of a node
// It holds the key and val in a node (leafPageElement or branchPageElement)
type inode struct {
	flags uint32 // leaf-page or sub-bucket
	pgid  pgid   // for branch-page's child's pgid
	key   []byte
	value []byte
}

type inodes []inode

func (n *node) put(key, value []byte, pgid pgid) {
	// find first larger index, precondition: increasing order
	idx := sort.Search(len(n.inodes), func(i int) bool {
		// n.inodes[i].key >= key
		return bytes.Compare(n.inodes[i].key, key) >= 0
	})

	if idx < len(n.inodes) && bytes.Equal(n.inodes[idx].key, key) {
		// key is present
	} else {
		// key is not present in increasing order data ([2,3,4])
		// and if key > maxKey, idx=len(nodes)+1
		// and if key < minKey, idx=0
		n.inodes = append(n.inodes, inode{})
		if idx == 0 {
			copy(n.inodes[idx+1:], n.inodes[idx:])
		}
	}

	inode := &n.inodes[idx]
	inode.key = key
	inode.value = value
	inode.pgid = pgid
}

func (n *node) read(p *page) {
	n.isLeaf = (p.flags & leafPageFlag) != 0
	n.inodes = make(inodes, p.count)

	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
	}
}

func (n *node) write(p *page) {
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	p.count = uint16(len(n.inodes))
	// no items need to write, just do nothing
	if p.count == 0 {
		return
	}

	// pageElement layout [pageElem1, pageElem2, pageElemData1, pageElemData1]
	pageElemDataOffset := pageHeaderSize + n.pageElementSize()*uintptr(len(n.inodes))
	for idx, item := range n.inodes {
		dataOffset := len(item.key) + len(item.value)
		pageElemData := unsafeByteSlice(unsafe.Pointer(p), pageElemDataOffset, 0, dataOffset)
		pageElemDataOffset += uintptr(dataOffset)

		if n.isLeaf {
			pageElem := p.leafPageElement(uint16(idx))
			pageElem.pos = uint32(uintptr(unsafe.Pointer(&pageElemData[0])) - uintptr(unsafe.Pointer(pageElem)))
			pageElem.flags = item.flags
			pageElem.ksize = uint32(len(item.key))
			pageElem.vsize = uint32(len(item.value))
		} else {
			pageElem := p.branchPageElement(uint16(idx))
			pageElem.pos = uint32(uintptr(unsafe.Pointer(&pageElemData[0])) - uintptr(unsafe.Pointer(pageElem)))
			pageElem.ksize = uint32(len(item.key))
			pageElem.pgid = item.pgid
		}

		// write pageElemData key
		copiedSize := copy(pageElemData, item.key)
		// write pageElemData value
		copy(pageElemData[copiedSize:], item.value)
	}
}

func (n *node) pageElementSize() uintptr {
	if n.isLeaf {
		return leafPageElementSize
	} else {
		return branchPageElementSize
	}
}
