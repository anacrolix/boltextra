package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/anacrolix/boltextra/boltutil"

	"github.com/boltdb/bolt"
)

var db *bolt.DB

func init() {
	// This is a quick hack for now.
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [repo options] repo command [command-args...]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nRepo options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nCommands:\n")
		for _, cmd := range []string{"keys", "buckets", "get", "put"} {
			fmt.Fprintf(os.Stderr, "  %s\n", cmd)
		}
		fmt.Fprintf(os.Stderr, "\n")
	}
}

func main() {
	create := flag.Bool("c", false, "create db file if it doesn't exist")
	flag.Parse()
	if !*create {
		_, err := os.Stat(flag.Args()[0])
		if err != nil {
			log.Fatal("db file does not exist")
		}
	}
	db, err := bolt.Open(flag.Args()[0], 0640, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	switch flag.Args()[1] {
	case "keys":
		err = db.View(func(tx *bolt.Tx) error {
			return boltutil.Path(tx, flag.Args()[2:]...).ForEach(func(k, v []byte) error {
				_, err := fmt.Fprintf(os.Stdout, "%s\n", k)
				return err
			})
		})
	default:
		log.Fatalf("unknown command: %s", flag.Args()[1])
	}
	if err != nil {
		log.Fatalf("error running command: %s", err)
	}
}
