package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"tinydb/db"
)

func main() {
	var schemaName string
	flag.StringVar(&schemaName, "schema", "", "filepath to a schema")
	flag.Parse()

	var scm db.Schema
	if schemaName != "" {
		s, err := db.LoadSchema(schemaName)
		if err != nil {
			log.Fatalf("failed to open schema %s  %s", schemaName, err)
		}
		scm = s
	}
	tdb = db.NewDatabase(scm)

	args := flag.Args()
	if len(args) > 0 {
		if err := db.Restore(args[0], tdb); err != nil {
			log.Fatalf("failed to restore database %s  %s", args[0], err)
		}
	}

	ctx, cnl := context.WithCancel(context.Background())
	defer cnl()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, os.Kill)
	done := make(chan bool)

	go readCommands(ctx, os.Stdout, done)
	for {
		select {
		case <-sig:
			return
		case <-done:
			log.Println("closed")
			return
		}
	}
}
