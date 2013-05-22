// Copyright 2013 Patrick Higgins.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdb

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestEmpty(t *testing.T) {
	db := createDB([]record{})

	for i, tc := range [][]byte{
		[]byte{},
		[]byte{0},
		[]byte{0, 1},
		[]byte("string key"),
	} {
		got, err := db.Data(tc)
		if err != ErrNotFound {
			t.Errorf("[%d] should have returned ErrNotFound, err: %v", i, err)
		}
		if got != nil {
			t.Errorf("[%d] should have returned nil: %v", i, got)
		}
	}
}

func TestCDB(t *testing.T) {
	var records []record
	for i := 0; i < 200000; i++ {
		key := []byte{
			byte(i & 0xff),
			byte((i >> 8) & 0xff),
			byte((i >> 16) & 0xff),
		}
		val := []byte{
			byte((i >> 16) & 0xff),
			byte((i >> 8) & 0xff),
			byte(i & 0xff),
		}
		records = append(records, record{key, val})
	}

	db := createDB(records)

	for i, rec := range records {
		got, err := db.Data(rec.key)
		if err != nil {
			t.Errorf("[%d] Data(%v): %v", i, rec.key, err)
			continue
		}
		if !bytes.Equal(got, rec.val) {
			t.Errorf("[%d] Data(%v)=%v, want=%v", i, rec.key, got, rec.val)
		}
	}
}

type record struct {
	key, val []byte
}

func createDB(records []record) *CDB {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	err = f.Close()
	if err != nil {
		panic(err)
	}

	err = Create(f.Name(), func(w io.Writer) error {
		data := make([]byte, 0, 8192)
		for _, rec := range records {
			data = data[:0]
			data = AppendRecord(data, []byte(rec.key), []byte(rec.val))
			_, err = w.Write(data)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	db, err := Open(f.Name())
	if err != nil {
		panic(err)
	}

	return db
}

func Example() {
	tmp, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal(err)
	}
	defer tmp.Close()
	defer os.Remove(tmp.Name())

	err = Create(tmp.Name(), func(cdbPipe io.Writer) error {
		buf := make([]byte, 0, 8192)
		for key, value := range map[string]string{
			"a": "123",
			"b": "456",
		} {
			buf = AppendRecord(buf[:0], []byte(key), []byte(value))
			_, err := cdbPipe.Write(buf)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	db, err := Open(tmp.Name())
	if err != nil {
		log.Fatal(err)
	}

	b, _ := db.Data([]byte("b"))
	fmt.Printf("%s\n", b)

	_, err = db.Data([]byte("c"))
	if err == ErrNotFound {
		fmt.Println("c not found")
	}

	// Output:
	// 456
	// c not found
}
