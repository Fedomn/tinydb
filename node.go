package tinydb

import (
	"bytes"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
type node struct {
	bucket   *Bucket
	isLeaf   bool
	spilled  bool
	key      []byte // first inode key
	pgid     pgid
	parent   *node
	children nodes
	inodes   inodes
}

// root returns the top-level node this node is attached to.
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

type nodes []*node

func (s nodes) Len() int      { return len(s) }
func (s nodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1
}

// inode represents an internal node inside of a node
// It holds the key and val in a node (leafPageElement or branchPageElement)
type inode struct {
	flags uint32 // leaf-page or sub-bucket
	pgid  pgid   // for branch-page's child's pgid
	key   []byte
	value []byte
}

type inodes []inode

func (n *node) put(oldKey, key, value []byte, pgid pgid, flags uint32) {
	// find first larger index, precondition: increasing order
	idx := sort.Search(len(n.inodes), func(i int) bool {
		// n.inodes[i].key >= oldKey
		return bytes.Compare(n.inodes[i].key, oldKey) >= 0
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
	inode.flags = flags
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

// spill writes the nodes to dirty pages and splits nodes as it goes.
func (n *node) spill() error {
	var tx = n.bucket.tx
	if n.spilled {
		return nil
	}

	// Spill child nodes first. Child nodes can materialize sibling nodes in
	// the case of split-merge so we cannot use a range loop. We have to check
	// the children size on every loop iteration.
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// We no longer need the child list because it's only used for spill tracking.
	n.children = nil

	// Split nodes into appropriate sizes. The first node will always be n.
	var nodes = n.split(uintptr(tx.db.pageSize))
	for _, node := range nodes {
		// TODO
		// free old pages
		// allocate new pages

		node.spilled = true

		// Insert into parent inodes.
		if node.parent != nil {
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}

			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key
		}
		// Update the statistics.
		tx.stats.Spill++
	}

	// If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill()
	}

	return nil
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
func (n *node) split(pageSize uintptr) []*node {
	var nodes []*node

	node := n
	for {
		// Split node into two.
		a, b := node.splitTwo(pageSize)
		nodes = append(nodes, a)

		// If we can't split then exit the loop.
		if b == nil {
			break
		}

		// Set node to b so it gets split on the next iteration.
		node = b
	}

	return nodes
}

const (
	minKeysPerPage = 2

	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
func (n *node) splitTwo(pageSize uintptr) (*node, *node) {
	// Ignore the split if the page doesn't have at least enough nodes for
	// two pages or if the nodes can fit in a single page.
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// Determine the threshold before starting a new node.
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// Determine split position and sizes of the two pages.
	splitIndex, _ := n.splitIndex(threshold)

	// Split node into two separate nodes.
	// If there's no parent then we'll need to create one.
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// Create a new node and add it to the parent.
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// Split inodes across two nodes.
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// Update the statistics.
	n.bucket.tx.stats.Split++

	return n, next
}

// sizeLessThan returns true if the node is less than a given size.
// This is an optimization to avoid calculating a large node when we only need
// to know if it fits inside a certain page size.
func (n *node) sizeLessThan(v uintptr) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + uintptr(len(item.key)) + uintptr(len(item.value))
		if sz >= v {
			return false
		}
	}
	return true
}

// splitIndex finds the position where a page will fill a given threshold.
// It returns the index as well as the size of the first page.
// This is only be called from split().
func (n *node) splitIndex(threshold int) (index, sz uintptr) {
	sz = pageHeaderSize

	// Loop until we only have the minimum number of keys required for the second page.
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = uintptr(i)
		inode := n.inodes[i]
		elsize := n.pageElementSize() + uintptr(len(inode.key)) + uintptr(len(inode.value))

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		if index >= minKeysPerPage && sz+elsize > uintptr(threshold) {
			break
		}

		// Add the element size to the total size.
		sz += elsize
	}

	return
}
