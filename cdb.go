// Copyright 2013 Patrick Higgins.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
)

const slotWidth = 8
const headerWidth = 8
const nHeaders = 256
const headerSize = nHeaders * headerWidth

// CDB is an open constant database file
type CDB struct {
	header [nHeaders]tablePointer
	data   []byte
}

type tablePointer struct {
	pos    uint32
	nslots uint32
}

var errShortFile = errors.New("file is too short for a CDB")

// Create writes a new CDB file to outfile. The file is first written to a
// temp file and atomically renamed. The external program "cdb" must be
// in your PATH.
//
// The creator callback should write records created with AppendRecord to
// the provided io.Writer.
func Create(outfile string, creator func(io.Writer) error) error {
	cdbCmd := exec.Command("cdb", "-c", outfile)
	cdbPipe, err := cdbCmd.StdinPipe()
	if err != nil {
		return nil
	}

	err = cdbCmd.Start()
	if err != nil {
		return err
	}

	var errs []string

	err = creator(cdbPipe)
	if err != nil {
		errs = append(errs, err.Error())
	}

	// complete the CDB records
	_, err = cdbPipe.Write([]byte("\n"))
	if err != nil {
		return err
	}

	err = cdbPipe.Close()
	if err != nil {
		errs = append(errs, "cdb close: "+err.Error())
	}

	err = cdbCmd.Wait()
	if err != nil {
		errs = append(errs, "cdb wait: "+err.Error())
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}

	return nil
}

// Open creates and returns a CDB from file.
func Open(file string) (*CDB, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	// it is OK to close a file after mapping it
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if fi.Size() == 0 {
		// an empty CDB is valid, but never finds any data
		// allowing this means /dev/null can be specified for data files that
		// we want to be empty.
		return &CDB{}, nil
	}

	if fi.Size() < headerSize {
		return nil, errShortFile
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()),
		syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	db := &CDB{
		data: data,
	}

	// munmap when the reference is released
	runtime.SetFinalizer(db, (*CDB).Close)

	db.readHeader(data)

	return db, nil
}

// Close releases resources associated with this CDB.
func (db *CDB) Close() error {
	var err error
	if db.data != nil {
		err = syscall.Munmap(db.data)
	}
	db.data = nil
	return err
}

var ErrNotFound = errors.New("key not found")

// Data returns the data associated with key. A shared copy of the data is
// returned and must not be modified. If not found, returns ErrNotFound.
func (db *CDB) Data(key []byte) (val []byte, err error) {
	// catch array range checks, etc.
	defer func() {
		if val := recover(); val != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.Printf("cdb: panic finding %v: %v\n%s", key, val, buf)
			// set named return value
			if perr, ok := val.(error); ok {
				err = perr
			} else {
				err = fmt.Errorf("cdb: panic in find: %v", val)
			}
		}
	}()

	hashcode := uint32(5381)
	for _, c := range key {
		hashcode = ((hashcode << 5) + hashcode) ^ uint32(c)
	}

	header := db.header[hashcode&0xff]
	if header.nslots == 0 {
		return nil, ErrNotFound
	}

	table := db.data[header.pos : header.pos+header.nslots*slotWidth]

	slot := ((hashcode >> 8) % header.nslots) * slotWidth

	// prevent endless loops if no slots are empty
	for i := uint32(0); i < header.nslots; i++ {
		hash := binary.LittleEndian.Uint32(table[slot:])
		pos := binary.LittleEndian.Uint32(table[slot+4:])
		if pos == 0 {
			return nil, ErrNotFound
		}
		if hash == hashcode {
			record := db.data[pos:]
			keyLen := binary.LittleEndian.Uint32(record)
			dataLen := binary.LittleEndian.Uint32(record[4:])
			recKey := record[8 : 8+keyLen]
			if bytes.Equal(key, recKey) {
				data := record[8+keyLen : 8+keyLen+dataLen]
				return data, nil
			}
		}

		// search next slot, wrapping around
		slot += slotWidth
		if slot >= uint32(len(table)) {
			slot = 0
		}
	}

	return nil, ErrNotFound
}

// readHeader populates db.header from data
func (db *CDB) readHeader(data []byte) {
	for i := range db.header {
		db.header[i].pos = binary.LittleEndian.Uint32(data)
		db.header[i].nslots = binary.LittleEndian.Uint32(data[4:])
		data = data[headerWidth:]
	}
}
