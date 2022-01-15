package minisql

import (
	"testing"
)

var testSchema = Schema{
	"t1": {
		"c1-1": true,
		"c1-2": true,
		"c1-3": true,
	},
	"t2": {
		"c2-1": true,
		"c2-2": true,
		"c2-3": true,
	},
	"t3": {
		"c3-1": true,
		"c3-2": true,
		"c3-3": true,
	},
}

func TestNewDatabase(t *testing.T) {
	db := NewDatabase(testSchema)
	if db == nil {
		t.Fatalf("NewDatabase returned nil")
	}
	tns := db.TableNames()
	if len(tns) != 3 {
		t.Fatalf("expected %d tables, found %d", len(testSchema), len(tns))
	}
}

func TestMiniDB_TableNames(t *testing.T) {
	db := NewDatabase(testSchema)
	tns := db.TableNames()
	if len(tns) != 3 {
		t.Fatalf("expected %d tables, found %d", len(testSchema), len(tns))
	}
	for _, tn := range tns {
		if _, ok := testSchema[tn]; !ok {
			t.Fatalf("%s is an unexpected table name", tn)
		}
	}
}

func TestMiniDB_ContainsTable(t *testing.T) {
	db := NewDatabase(testSchema)
	for tn := range testSchema {
		if !db.ContainsTable(tn) {
			t.Fatalf("%s is an unexpected table name", tn)
		}
	}
	if db.ContainsTable("Not there") {
		t.Fatalf("contains table returned true for non existing table")
	}
}

func TestMiniDB_Describe(t *testing.T) {
	db := NewDatabase(testSchema)
	for tn := range testSchema {
		cns, err := db.Describe(tn)
		if err != nil {
			t.Fatalf("error describing table %s  %s", tn, err)
		}
		sm := testSchema[tn]
		if len(sm) != len(cns) {
			t.Fatalf("unexpected number of columns in table %s.  Expected %d, found %d", tn, len(sm), len(cns))
		}
		for _, cn := range cns {
			if !sm[cn] {
				t.Fatalf("unexpected column name %s in table %s", cn, tn)
			}
		}
	}
}
