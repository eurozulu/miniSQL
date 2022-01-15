package minisql

import (
	"fmt"
)

type Key int64

type MiniDB struct {
	tables map[string]Table
}

func (db MiniDB) TableNames() []string {
	names := make([]string, len(db.tables))
	var i int
	for tn := range db.tables {
		names[i] = tn
		i++
	}
	return names
}

func (db MiniDB) ContainsTable(tablename string) bool {
	_, ok := db.tables[tablename]
	return ok
}

func (db MiniDB) Table(tablename string) (Table, error) {
	t, ok := db.tables[tablename]
	if !ok {
		return nil, fmt.Errorf("%q is not a known table", tablename)
	}
	return t, nil
}

func (db MiniDB) Describe(tablename string) ([]string, error) {
	t, ok := db.tables[tablename]
	if !ok {
		return nil, fmt.Errorf("%s is an unknown table", tablename)
	}
	return t.ColumnNames(), nil
}

func (db *MiniDB) AlterDatabase(schema Schema) {
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
		t = newTable(cols)
		if len(t.ColumnNames()) > 0 {
			db.tables[tn] = t
		}
	}
}

func NewDatabase(schema Schema) *MiniDB {
	db := &MiniDB{tables: map[string]Table{}}
	if schema != nil {
		db.AlterDatabase(schema)
	}
	return db
}
