package tinydb

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

type Schema map[string]map[string]bool

func (s Schema) Save(filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer func(f io.WriteCloser) {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}(f)
	return json.NewEncoder(f).Encode(&s)
}

func LoadSchema(filepath string) (Schema, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer func(f io.ReadCloser) {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}(f)
	sc := Schema{}
	if err = json.NewDecoder(f).Decode(&sc); err != nil {
		return nil, err
	}
	return sc, nil
}
