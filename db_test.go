package tinydb

import (
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
