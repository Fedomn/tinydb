package tinydb

import (
	"testing"
)

func TestNode_Put(t *testing.T) {
	n := node{inodes: make(inodes, 0)}
	n.put([]byte("k2"), []byte("v2"), 0)
	n.put([]byte("k3"), []byte("v3"), 0)
	n.put([]byte("k1"), []byte("v1"), 0)
	n.put([]byte("k1"), []byte("v4"), 0)

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
