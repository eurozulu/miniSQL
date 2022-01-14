package tinydb

import (
	"encoding/json"
	"eurozulu/tinydb/stringutil"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
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

func NewSchemaFromTables(db *TinyDB, tableName ...string) (Schema, error) {
	if len(tableName) == 0 {
		return nil, fmt.Errorf("must proive at least one table to generate schema from")
	}
	sc := Schema{}
	for _, tn := range tableName {
		t, err := db.Table(tn)
		if err != nil {
			return nil, err
		}
		cols := map[string]bool{}
		for _, cn := range t.ColumnNames() {
			cols[cn] = true
		}
		sc[tn] = cols
	}
	return sc, nil
}

func NewSchema(schema string) (Schema, error) {
	table, rest := stringutil.FirstWord(schema)
	if table == "" {
		return nil, fmt.Errorf("no table name found")
	}
	if rest == "" {
		return nil, fmt.Errorf("no columns stated for table %q", table)
	}
	var colList string
	colList, rest = stringutil.BracketedString(rest)
	if colList == "" {
		return nil, fmt.Errorf("no columns in brackets stated for table %q", table)
	}
	cols := map[string]bool{}
	for _, col := range strings.Split(colList, ",") {
		cols[strings.TrimSpace(col)] = true
	}
	return Schema{table: cols}, nil
}
