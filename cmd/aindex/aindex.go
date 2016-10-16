package main

import (
	"flag"
	"github.com/acoustid/go-acoustid/index"
	"log"
)

func main() {
	bind := flag.String("bind", "localhost:7765", "port number on which to listen")
	dirFlag := flag.String("dir", "index_data", "path to the database directory")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	idx, err := index.Open(*dirFlag)
	if err != nil {
		log.Fatalf("failed to open the index (%s)", err)
	}
	defer idx.Close()

	log.Fatal(index.ListenAndServe(*bind, idx))
}
