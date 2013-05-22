// Copyright 2013 Patrick Higgins.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdb

import (
	"strconv"
)

// AppendString appends a single byte length followed by the UTF-8 bytes of
// s to data and returns the appended slice. Will panic if len(s) exceeds
// 255.
func AppendString(data []byte, s string) []byte {
	b := []byte(s)
	l := uint8(len(b))
	if l > 255 {
		panic("string too long: " + s)
	}
	data = append(data, l)
	data = append(data, b...)
	return data
}

// ReadString is the inverse of AppendString. It reads a byte length and UTF-8
// bytes from data and returns the string and the number of bytes consumed from
// data, including both the length byte and UTF-8 string bytes.
func ReadString(data []byte) (string, int) {
	slen := uint8(data[0])
	s := string(data[1 : slen+1])
	return s, int(slen + 1)
}

// AppendRecord appends the (key,value) record to data and returns the
// appended slice.
func AppendRecord(data, key, value []byte) []byte {
	data = append(data, '+')
	data = strconv.AppendUint(data, uint64(len(key)), 10)
	data = append(data, ',')
	data = strconv.AppendUint(data, uint64(len(value)), 10)
	data = append(data, ':')
	data = append(data, key...)
	data = append(data, []byte("->")...)
	data = append(data, value...)
	return append(data, '\n')
}
