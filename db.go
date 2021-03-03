package tinydb

import (
	"os"
	"unsafe"
)

type Db struct {
	path     string
	file     *os.File
	pageSize int
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
