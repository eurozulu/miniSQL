package tinydb

import (
	"fmt"
	"testing"
)

func TestColumn_Insert(t *testing.T) {
	col := column{}
	if err := col.Insert(1, "test"); err != nil {
		t.Fatalf("error inserting into column  %s", err)
	}

	v, ok := col[1]
	if !ok {
		t.Fatalf("inserted value not found")
	}
	if v != "test" {
		t.Fatalf("unexpected value found, expected 'test', found '%s'", v)
	}

	if err := col.Insert(1, "test"); err == nil {
		t.Fatalf("expected error inserting existing ID")
	}

}

func TestColumn_Update(t *testing.T) {
	col := column{}
	col.Update(1, "test")
	v, ok := col[1]
	if !ok {
		t.Fatalf("inserted value not found")
	}
	if v != "test" {
		t.Fatalf("unexpected value found, expected 'test', found '%s'", v)
	}
	col.Update(1, "tost")
	v, ok = col[1]
	if !ok {
		t.Fatalf("inserted value not found")
	}
	if v != "tost" {
		t.Fatalf("unexpected value found, expected 'tost', found '%s'", v)
	}
}

func TestColumn_Delete(t *testing.T) {
	col := column{}
	col.Update(1, "test")
	_, ok := col[1]
	if !ok {
		t.Fatalf("inserted value not found")
	}
	if err := col.Delete(2); err == nil {
		t.Fatalf("expected error deleting non existing key")
	}
	if err := col.Delete(1); err != nil {
		t.Fatalf("unexpected error deleting existing key %s", err)
	}
	if len(col) != 0 {
		t.Fatalf("Expected empty column after deletion")
	}
}

func TestColumn_Find(t *testing.T) {
	col := column{}
	col.Update(1, "test1")
	col.Update(2, "test2")
	col.Update(3, "test3")
	keys := col.Find("test")
	if len(keys) != 0 {
		t.Fatalf("unexpected Keys found from Find with non existing value")
	}
	for i := 1; i < 4; i++ {
		f := fmt.Sprintf("test%d", i)
		keys = col.Find(f)
		if len(keys) != 1 {
			t.Fatalf("expected one key found, found %d, searching for %s", len(keys), f)
		}
		v := col[keys[0]]
		if v != fmt.Sprintf("test%d", keys[0]) {
			t.Fatalf("unexpected value found, expected %s, found %s", fmt.Sprintf("test%d", keys[0]), v)
		}
	}
}
