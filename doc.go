// Copyright 2013 Patrick Higgins.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Reads and writes D. J. Bernstein's constant database format
// (CDB). See http://cr.yp.to/cdb.html for details on the format.
//
// This package is intended for read-only data and is useful to get
// large data sets out of the heap so they will not impact the
// performance of the Go garbage collector.
//
// The data is loaded using mmap(2) and shared copies are returned,
// which is why the data must be read-only.
//
// The package delegates creation of CDB files to an external "cdb"
// utility, which must be installed. Both Bernstein's original cdb
// and TinyCDB (http://www.corpit.ru/mjt/tinycdb.html) may be used.
package cdb
