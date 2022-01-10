package main

import (
	"context"
	"eurozulu/tinydb/commands"
	"eurozulu/tinydb/tinydb"
	"flag"
	"log"
	"os"
	"os/signal"
)

func main() {
	var dbPath string
	var schemaName string
	flag.StringVar(&dbPath, "database", "", "filepath to a dump file of a database to load")
	flag.StringVar(&schemaName, "schema", "", "filepath to a schema")
	flag.Parse()

	var scm tinydb.Schema
	if schemaName != "" {
		s, err := tinydb.LoadSchema(schemaName)
		if err != nil {
			log.Fatalf("failed to open schema %s  %s", schemaName, err)
		}
		scm = s
	}
	commands.Database = tinydb.NewDatabase(scm)

	if dbPath != "" {
		if err := commands.RestoreCommand(dbPath, os.Stdout); err != nil {
			log.Fatalf("failed to restore database %s  %s", dbPath, err)
		}
	}

	//args := flag.Args()

	ctx, cnl := context.WithCancel(context.Background())
	defer cnl()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, os.Kill)
	done := make(chan bool)

	go commands.RunCommands(ctx, os.Stdout, done)
	for {
		select {
		case <-sig:
			return
		case <-done:
			return
		}
	}
}
