package main

import (
	"context"
	"eurozulu/tinydb/commands"
	"eurozulu/tinydb/db"
	"flag"
	"log"
	"os"
	"os/signal"
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
	commands.Database = db.NewDatabase(scm)

	args := flag.Args()
	if len(args) > 0 {
		if err := commands.RestoreCommand(args[0], os.Stdout); err != nil {
			log.Fatalf("failed to restore database %s  %s", args[0], err)
		}
	}

	ctx, cnl := context.WithCancel(context.Background())
	defer cnl()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, os.Kill)
	done := make(chan bool)

	go commands.ReadCommands(ctx, os.Stdout, done)
	for {
		select {
		case <-sig:
			return
		case <-done:
			return
		}
	}
}
