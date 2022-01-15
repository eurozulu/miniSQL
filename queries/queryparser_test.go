package queries

import (
	"context"
	"eurozulu/miniSQL/minisql"
	"reflect"
	"testing"
)

var testSchema = minisql.Schema{
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

func TestQueryParser_ParseInsertValues(t *testing.T) {
	query := "INSERT INTO t1 (c1-1, c1-2, c1-3) VALUES('one', 'two', 'three')"
	q, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query %s", err)
	}
	if _, ok := q.(*InsertQuery); !ok {
		t.Fatalf("unexpected query type found.  Expected %s, found %s", "*InsertQuery", reflect.TypeOf(q).Elem().Name())
	}
	tdb := minisql.NewDatabase(testSchema)
	rCh, err := q.Execute(context.TODO(), tdb)
	if err != nil {
		t.Fatalf("failed to execute query %s", err)
	}
	r := <-rCh
	if r == nil {
		t.Fatalf("unexpected nil result")
	}
	if r.TableName() != "t1" {
		t.Fatalf("unexpected table name in result.  Expected %s, found %s", "t1", r.TableName())
	}
	id, ok := r.Values()["_id"]
	if !ok {
		t.Fatalf("expected _id not found")
	}
	if id == nil || *id != "0" {
		t.Fatalf("unexpected result id, expected '0', found %v", id)
	}
	_, ok = <-rCh
	if ok {
		t.Fatalf("Expected result channel to close")
	}
}

func TestQueryParser_ParseSelect(t *testing.T) {
	query := "SELECT * FROM t1"
	q, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query %s", err)
	}
	if _, ok := q.(*SelectQuery); !ok {
		t.Fatalf("unexpected query type found.  Expected %s, found %s", "*SelectQuery", reflect.TypeOf(q).Elem().Name())
	}
	tdb := minisql.NewDatabase(testSchema)
	_, err = q.Execute(context.TODO(), tdb)
	if err != nil {
		t.Fatalf("failed to execute query %s", err)
	}
}
