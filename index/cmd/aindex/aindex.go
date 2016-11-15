// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package main

import (
	"flag"
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/index/vfs"
	"log"
	"net"
	"strconv"
)

var (
	hostOpt   = flag.String("host", "localhost", "host on which to listen")
	portOpt   = flag.Int("port", 7765, "port number on which to listen")
	dbPathOpt = flag.String("dbpath", "", "path to the database directory")
)

func main() {
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	var fs vfs.FileSystem
	if *dbPathOpt == "" {
		fs = vfs.CreateMemDir()
	} else {
		var err error
		fs, err = vfs.OpenDir(*dbPathOpt, true)
		if err != nil {
			log.Fatalf("Failed to open the database directory: %v", err)
		}
	}

	log.Printf("Opening database in %v", fs)
	idx, err := index.Open(fs, true)
	if err != nil {
		log.Fatalf("Failed to open the database: %v", err)
	}
	defer idx.Close()

	addr := net.JoinHostPort(*hostOpt, strconv.Itoa(*portOpt))
	log.Printf("Listening on %v", addr)

	err = index.ListenAndServe(addr, idx)
	if err != nil {
		log.Fatalf("Failed to start the server: %v", err)
	}
}
