package db

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type Key int64
type Schema map[string]map[string]bool

type TinyDB struct {
	tables map[string]Table
}

func (db TinyDB) TableNames() []string {
	names := make([]string, len(db.tables))
	var i int
	for tn := range db.tables {
		names[i] = tn
		i++
	}
	return names
}

func (db TinyDB) ContainsTable(tablename string) bool {
	_, ok := db.tables[tablename]
	return ok
}

func (db TinyDB) Describe(tablename string) ([]string, error) {
	t, ok := db.tables[tablename]
	if !ok {
		return nil, fmt.Errorf("%s is an unknown table", tablename)
	}
	return t.ColumnNames(), nil
}

func (db *TinyDB) AlterDatabase(schema Schema) {
	for tn, cols := range schema {
		if len(cols) == 0 {
			// drop table with no columns
			delete(db.tables, tn)
			continue
		}

		t, ok := db.tables[tn]
		if ok {
			// table already exists
			t.AlterColumns(cols)
			continue
		}
		db.tables[tn] = newTable(cols)
	}
}
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

func NewDatabase(schema Schema) *TinyDB {
	db := &TinyDB{tables: map[string]Table{}}
	if schema != nil {
		db.AlterDatabase(schema)
	}
	return db
}
