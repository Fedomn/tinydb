package tinydb

import (
	"fmt"
	"os"
	"sync"
	"unsafe"
)

// maxMapSize represents the largest mmap size supported by Bolt.
const maxMapSize = 0xFFFFFFFFFFFF // 256TB
// The largest step that can be taken when remapping the mmap.
const maxMmapStep = 1 << 30 // 1GB

type Db struct {
	path     string
	file     *os.File
	dataref  []byte // mmap'ed readonly, write throws SEGV
	data     *[maxMapSize]byte
	datasz   int
	pageSize int
	freelist *freelist
	pagePool sync.Pool
	rwtx     *Tx

	meta0 *meta
	meta1 *meta

	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.
}

const tinyDBVersion = 1
const fileMode = 0666

// default page size for db is set to the OS page size.
var defaultPageSize = os.Getpagesize()

func Open(path string) (*Db, error) {
	db := &Db{
		pageSize: defaultPageSize,
	}
	flag := os.O_RDWR | os.O_CREATE

	// open data file
	var err error
	if db.file, err = os.OpenFile(path, flag, fileMode); err != nil {
		return nil, err
	}
	db.path = db.file.Name()

	// initialize the database if it doesn't exist
	if fileInfo, err := db.file.Stat(); err != nil {
		return nil, err
	} else if fileInfo.Size() == 0 {
		// initialize meta pages
		if err := db.init(); err != nil {
			_ = db.file.Close()
			return nil, err
		}
	} else {
		// read meta page to validate
		var buf [4069]byte
		bw, err := db.file.ReadAt(buf[:], 0)
		if err == nil && bw == len(buf) {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err = m.validate(); err != nil {
				return nil, err
			}
		} else {
			return nil, ErrInvalid
		}
	}

	return db, nil
}

// init creates a new database file and initialize its meta pages.
func (db *Db) init() error {
	buf := make([]byte, db.pageSize*4)
	// first create two meta pages
	for i := 0; i < 2; i++ {
		// page struct will memory alignment
		// buf[:] to get slice struct array address
		page := db.pageInBuffer(buf[:], i)
		page.id = pgid(i)

		// init meta page
		m := page.meta()
		m.pgid = pgid(i)
		m.pageSize = uint32(db.pageSize)
		m.version = tinyDBVersion
		m.checksum = m.sum64()
	}

	// create a freelist page
	p := db.pageInBuffer(buf[:], 2)
	p.id = pgid(2)

	// create a empty leaf page for preparation
	p = db.pageInBuffer(buf[:], 3)
	p.id = pgid(2)

	if _, err := db.file.Write(buf); err != nil {
		return err
	}

	if err := db.file.Sync(); err != nil {
		return err
	}

	return nil
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *Db) pageInBuffer(b []byte, id int) *page {
	return (*page)(unsafe.Pointer(&b[id*db.pageSize]))
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *Db) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *Db) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// Use pages from the freelist if they are available.
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// Move the page id high water mark.
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *Db) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// Dereference all mmap references before unmapping.
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// Unmap existing data before continuing.
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	if err := mmap(db, size); err != nil {
		return err
	}

	// Save references to the meta pages.
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// munmap unmaps the data file from memory.
func (db *Db) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
func (db *Db) mmapSize(size int) (int, error) {
	// Double the size from 32KB until 1GB.
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}
