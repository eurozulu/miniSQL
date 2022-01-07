package db

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

func Dump(filename string, tdb *TinyDB) error {
	f, err := os.Open(filename)
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
	return json.NewDecoder(f).Decode(&tdb.tables)
}
