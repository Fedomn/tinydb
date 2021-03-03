package tinydb

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"unsafe"
)

// tempfile returns a temporary file path.
func tempfile() string {
	f, err := ioutil.TempFile("", "tinydb-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}

func TestOpen_InitDBFile(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	dbFileSize := fileInfo.Size()
	expectedDBFileSize := int64(os.Getpagesize() * 4)
	if expectedDBFileSize != dbFileSize {
		t.Fatalf("incorrect init db file size %d, expected size: %d", dbFileSize, expectedDBFileSize)
	}

	readFileBuf, _ := ioutil.ReadFile(path)
	firstPage := (*page)(unsafe.Pointer(&readFileBuf[os.Getpagesize()]))
	if err := firstPage.meta().validate(); err != nil {
		t.Fatalf("incorrect page meta %v", err)
	}
}

func TestOpen_ErrInvalid(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(f, "this is not a tinydb database"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); err != ErrInvalid {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestOpen_ExistFile(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	_, _ = Open(path)

	_, err := Open(path)
	if err != nil {
		t.Fatalf("Open exist tinydb file error")
	}
}

func TestOpen_ErrVersionMismatch(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	_, _ = Open(path)

	// Read data file.
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// current page only has id field which is uint64 = 8byte
	pageHeaderSize := int(unsafe.Sizeof(page{}))
	pageSize := os.Getpagesize()

	// Rewrite meta pages.
	meta0 := (*meta)(unsafe.Pointer(&buf[pageHeaderSize]))
	meta0.version++
	meta1 := (*meta)(unsafe.Pointer(&buf[pageSize+pageHeaderSize]))
	meta1.version++
	if err := ioutil.WriteFile(path, buf, 0666); err != nil {
		t.Fatal(err)
	}

	// Reopen data file.
	if _, err := Open(path); err != ErrVersionMismatch {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestOpen_ErrChecksum(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	_, _ = Open(path)

	// Read data file.
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// current page only has id field which is uint64 = 8byte
	pageHeaderSize := int(unsafe.Sizeof(page{}))
	pageSize := os.Getpagesize()

	// Rewrite meta pages.
	meta0 := (*meta)(unsafe.Pointer(&buf[pageHeaderSize]))
	meta0.pgid++
	meta1 := (*meta)(unsafe.Pointer(&buf[pageSize+pageHeaderSize]))
	meta1.pgid++
	if err := ioutil.WriteFile(path, buf, 0666); err != nil {
		t.Fatal(err)
	}

	// Reopen data file.
	if _, err := Open(path); err != ErrChecksum {
		t.Fatalf("unexpected error: %s", err)
	}
}
