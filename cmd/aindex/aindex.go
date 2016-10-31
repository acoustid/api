package main

import (
	"flag"
	"fmt"
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/index/vfs"
	"log"
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
			log.Fatalf("Failed to open the index (%s)", err)
		}
	}

	idx, err := index.Open(fs, true)
	if err != nil {
		log.Fatalf("failed to open the index (%s)", err)
	}
	defer idx.Close()

	addr := fmt.Sprintf("%s:%d", *hostOpt, *portOpt)
	log.Fatal(index.ListenAndServe(addr, idx))
}
