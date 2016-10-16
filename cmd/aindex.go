package main

import (
	"flag"
	"log"
	"github.com/acoustid/go-backend/index"
)

func main() {
	dirFlag := flag.String("dir", "index_data", "path to the database directory")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("Hello world %s", *dirFlag)

	idx, err := index.Open(*dirFlag)
	if err != nil {
		log.Fatalf("failed to open the index (%s)", err)
	}
	defer idx.Close()

	log.Fatal(index.ListenAndServe(":8080", idx))
}
