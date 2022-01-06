package db

type Key int64
type Schema map[string]map[string]bool

type TinyDB struct {
	tables map[string]*table
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

func (db TinyDB) ContainsTable(name string) bool {
	_, ok := db.tables[name]
	return ok
}

func (db *TinyDB) AlterTable(schema Schema) {
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
		db.tables[tn] = NewTable(cols)
	}
}
