package tinydb

import (
	"fmt"
	"os"
	"unsafe"
)

type Db struct {
	path     string
	file     *os.File
	pageSize int
}

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
		// TODO
	}

	return db, nil
}

// init creates a new database file and initialize its meta pages.
func (db *Db) init() error {
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 4; i++ {
		// page struct will memory alignment
		// buf[:] to get slice struct array address
		page := db.pageInBuffer(buf[:], i)
		page.id = uint64(i)
		page.desc = fmt.Sprintf("page-%d", i)
	}

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