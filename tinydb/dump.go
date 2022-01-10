package tinydb

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

func Dump(filename string, tdb *TinyDB) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}
	defer func(f io.WriteCloser) {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}(f)
	return json.NewEncoder(f).Encode(&tdb.tables)
}

func Restore(filename string, tdb *TinyDB) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func(f io.ReadCloser) {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}(f)

	tables := map[string]*table{}
	if err := json.NewDecoder(f).Decode(&tables); err != nil {
		return err
	}
	for k, t := range tables {
		tdb.tables[k] = t
	}
	return nil
}
