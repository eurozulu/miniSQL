package db

import "testing"

func TestNewTable(t *testing.T) {
	cols := map[string]bool{"one": true, "two": true, "three": true}
	tb := newTable(cols)
	if tb == nil {
		t.Fatalf("Newtable returned nil")
	}
}

func TestTable_ColumnNames(t *testing.T) {
	cols := map[string]bool{"one": true, "two": true, "three": true}
	tb := newTable(cols)

	tns := tb.ColumnNames()
	if len(tns) != len(cols) {
		t.Fatalf("Expected %d columns, found %d", len(cols), len(tns))
	}
	for _, tn := range tns {
		if !cols[tn] {
			t.Fatalf("unexpected column name %s", tn)
		}
	}
}

func TestTable_AlterColumns(t *testing.T) {
	cols := map[string]bool{"one": true, "two": true, "three": true}
	tb := newTable(cols)
	cns := tb.ColumnNames()
	if len(cns) != len(cols) {
		t.Fatalf("Expected %d columns, found %d", len(cols), len(cns))
	}

	cols["two"] = false
	tb.AlterColumns(cols)
	cns = tb.ColumnNames()
	if len(cns) != (len(cols) - 1) {
		t.Fatalf("Expected %d columns, found %d", (len(cols) - 1), len(cns))
	}
	for _, tn := range cns {
		if !cols[tn] {
			t.Fatalf("unexpected column name %s", tn)
		}
	}
}

func TestTable_NextID(t *testing.T) {
	cols := map[string]bool{"one": true, "two": true, "three": true}
	vals := []string{"1", "2", "3"}

	tb := newTable(cols)
	k := tb.NextID()
	if k != 0 {
		t.Fatalf("Expected zero next id on empty table, found %d", k)
	}
	tb.Insert(Values{"one": &vals[0]})
	k = tb.NextID()
	if k != 1 {
		t.Fatalf("Expected %d next id, found %d", 1, k)
	}
	tb.Insert(Values{"two": &vals[1]})
	tb.Insert(Values{"three": &vals[2]})
	k = tb.NextID()
	if k != 3 {
		t.Fatalf("Expected %d next id, found %d", 3, k)
	}
	tb.Delete(1)
	k = tb.NextID()
	if k != 3 {
		t.Fatalf("Expected %d next id, found %d", 3, k)
	}
	tb.Delete(0)
	tb.Delete(2)
	k = tb.NextID()
	if k != 3 {
		t.Fatalf("Expected %d next id, found %d", 3, k)
	}
}
