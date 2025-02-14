package tinydb

import (
	"testing"
	"unsafe"
)

func TestNode_Put(t *testing.T) {
	n := node{inodes: make(inodes, 0)}
	n.put([]byte("k2"), []byte("k2"), []byte("v2"), 0, 0)
	n.put([]byte("k3"), []byte("k3"), []byte("v3"), 0, 0)
	n.put([]byte("k1"), []byte("k1"), []byte("v1"), 0, 0)
	n.put([]byte("k1"), []byte("k1"), []byte("v4"), 0, 0)

	if len(n.inodes) != 3 {
		t.Fatalf("expect inodes is 3; got %d", len(n.inodes))
	}

	k := string(n.inodes[0].key)
	v := string(n.inodes[0].value)
	if k != "k1" || v != "v4" {
		t.Fatalf("expect k1 at 0, got %s:%s", k, v)
	}

	k = string(n.inodes[1].key)
	v = string(n.inodes[1].value)
	if k != "k2" || v != "v2" {
		t.Fatalf("expect k2 at 1, got %s:%s", k, v)
	}

	k = string(n.inodes[2].key)
	v = string(n.inodes[2].value)
	if k != "k3" || v != "v3" {
		t.Fatalf("expect k3 at 0, got %s:%s", k, v)
	}
}

func TestNode_ReadLeafPage(t *testing.T) {
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = leafPageFlag
	page.count = 2

	pageHeaderStart := uintptr(unsafe.Pointer(page))
	pageElementsStart := pageHeaderStart + pageHeaderSize

	// construct page elements:
	// pageElements space layout:
	// [pageElem1, pageElem2, kv1, vk2]
	// so pos is sequential added val
	pageElements := (*[2]leafPageElement)(unsafe.Pointer(pageElementsStart))
	pageElements[0] = leafPageElement{
		flags: leafPageFlag,
		pos:   uint32(leafPageElementSize * 2), // kv1 behind [pageElem1, pageElem2]
		ksize: 4,
		vsize: 4,
	}
	pageElements[1] = leafPageElement{
		flags: leafPageFlag,
		pos:   uint32(leafPageElementSize) + pageElements[0].ksize + pageElements[0].vsize,
		ksize: 4,
		vsize: 4,
	}

	// write data to above page elements
	s := "key1" + "val1" + "key2" + "val2"
	data := unsafeByteSlice(unsafe.Pointer(pageElementsStart), leafPageElementSize*2, 0, len(s))
	copy(data, s)

	// deserialize page
	n := &node{}
	n.read(page)

	if !n.isLeaf {
		t.Fatalf("expect leaf")
	}

	if len(n.inodes) != 2 {
		t.Fatalf("expect inodes count is 2, got %d", len(n.inodes))
	}

	k := string(n.inodes[0].key)
	v := string(n.inodes[0].value)
	if k != "key1" || v != "val1" {
		t.Fatalf("expect inode-1: key1:val1 , got %s:%s", k, v)
	}

	k = string(n.inodes[1].key)
	v = string(n.inodes[1].value)
	if k != "key2" || v != "val2" {
		t.Fatalf("expect inode-2: key2:val2 , got %s:%s", k, v)
	}
}

func TestNode_WriteLeafPage(t *testing.T) {
	n1 := &node{
		isLeaf: true,
		inodes: make(inodes, 0),
	}
	n1.put([]byte("k2"), []byte("k2"), []byte("v2"), 0, 0)
	n1.put([]byte("k3"), []byte("k3"), []byte("v3"), 0, 0)
	n1.put([]byte("k1"), []byte("k1"), []byte("v1"), 0, 0)
	n1.put([]byte("k1"), []byte("k1"), []byte("v4"), 0, 0)

	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	n1.write(p)

	n := &node{}
	n.read(p)

	if len(n.inodes) != 3 {
		t.Fatalf("expect inodes is 3; got %d", len(n.inodes))
	}

	k := string(n.inodes[0].key)
	v := string(n.inodes[0].value)
	if k != "k1" || v != "v4" {
		t.Fatalf("expect k1 at 0, got %s:%s", k, v)
	}

	k = string(n.inodes[1].key)
	v = string(n.inodes[1].value)
	if k != "k2" || v != "v2" {
		t.Fatalf("expect k2 at 1, got %s:%s", k, v)
	}

	k = string(n.inodes[2].key)
	v = string(n.inodes[2].value)
	if k != "k3" || v != "v3" {
		t.Fatalf("expect k3 at 0, got %s:%s", k, v)
	}
}

// Ensure that a node can split into appropriate subgroups.
func TestNode_split(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &Db{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)

	// Split between 2 & 3.
	n.split(100)

	var parent = n.parent
	if len(parent.children) != 2 {
		t.Fatalf("exp=2; got=%d", len(parent.children))
	}
	if len(parent.children[0].inodes) != 2 {
		t.Fatalf("exp=2; got=%d", len(parent.children[0].inodes))
	}
	if len(parent.children[1].inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(parent.children[1].inodes))
	}
}

// Ensure that a page with the minimum number of inodes just returns a single node.
func TestNode_split_MinKeys(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &Db{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)

	// Split.
	n.split(20)
	if n.parent != nil {
		t.Fatalf("expected nil parent")
	}
}

// Ensure that a node that has keys that all fit on a page just returns one leaf.
func TestNode_split_SinglePage(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &Db{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)

	// Split.
	n.split(4096)
	if n.parent != nil {
		t.Fatalf("expected nil parent")
	}
}
