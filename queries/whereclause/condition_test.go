package whereclause_test

import (
	"eurozulu/miniSQL/minisql"
	"eurozulu/miniSQL/queries/whereclause"
	"strings"
	"testing"
)

func TestCondition_Parse(t *testing.T) {
	c, rest, err := whereclause.ParseCondition("a=b")
	if err != nil {
		t.Fatalf("Failed to parse %v", err)
	}
	if rest != "" {
		t.Fatalf("unexpected string found %q in 'rest' after parse", rest)
	}
	if c == nil {
		t.Fatalf("parsed condition was nil")
	}
	if c.Column != "a" {
		t.Fatalf("unexpected column name, expected %q found %q", "a", c.Column)
	}
	if c.Operator != whereclause.OP_EQUAL {
		t.Fatalf("unexpected operator, expected %q, found %q", whereclause.OP_EQUAL, c.Operator)
	}
	if c.Value == nil {
		t.Fatalf("unexpected nil value, expected %q", "b")
	}
	if *c.Value != "b" {
		t.Fatalf("unexpected value, expected %q found %q", "b", *c.Value)
	}

	c, rest, err = whereclause.ParseCondition("a = b")
	if err != nil {
		t.Fatalf("Failed to parse %v", err)
	}
	if rest != "" {
		t.Fatalf("unexpected string found %q in 'rest' after parse", rest)
	}
	if c == nil {
		t.Fatalf("parsed condition was nil")
	}
	if c.Column != "a" {
		t.Fatalf("unexpected column name, expected %q found %q", "a", c.Column)
	}
	if c.Operator != whereclause.OP_EQUAL {
		t.Fatalf("unexpected operator, expected %q, found %q", whereclause.OP_EQUAL, c.Operator)
	}
	if c.Value == nil {
		t.Fatalf("unexpected nil value, expected %q", "b")
	}
	if *c.Value != "b" {
		t.Fatalf("unexpected value, expected %q found %q", "b", *c.Value)
	}

	c, rest, err = whereclause.ParseCondition("a != 'haha'")
	if err != nil {
		t.Fatalf("Failed to parse %v", err)
	}
	if rest != "" {
		t.Fatalf("unexpected string found %q in 'rest' after parse", rest)
	}
	if c == nil {
		t.Fatalf("parsed condition was nil")
	}
	if c.Column != "a" {
		t.Fatalf("unexpected column name, expected %q found %q", "a", c.Column)
	}
	if c.Operator != whereclause.OP_NOT_EQUAL_ALT {
		t.Fatalf("unexpected operator, expected %q, found %q", whereclause.OP_NOT_EQUAL_ALT, c.Operator)
	}
	if c.Value == nil {
		t.Fatalf("unexpected nil value, expected %q", "b")
	}
	if *c.Value != "haha" {
		t.Fatalf("unexpected value, expected %q found %q", "haha", *c.Value)
	}

	c, rest, err = whereclause.ParseCondition("a != 'haha' AND Then some")
	if err != nil {
		t.Fatalf("Failed to parse %v", err)
	}
	if rest != " AND Then some" {
		t.Fatalf("unexpected string found 'rest' expected %q, found %q", " AND Then some", rest)
	}
	if c == nil {
		t.Fatalf("parsed condition was nil")
	}
	if c.Column != "a" {
		t.Fatalf("unexpected column name, expected %q found %q", "a", c.Column)
	}
	if c.Operator != whereclause.OP_NOT_EQUAL_ALT {
		t.Fatalf("unexpected operator, expected %q, found %q", whereclause.OP_NOT_EQUAL_ALT, c.Operator)
	}
	if c.Value == nil {
		t.Fatalf("unexpected nil value, expected %q", "b")
	}
	if *c.Value != "haha" {
		t.Fatalf("unexpected value, expected %q found %q", "haha", *c.Value)
	}

	c, rest, err = whereclause.ParseCondition("!= 'haha'")
	if err == nil {
		t.Fatalf("expected exception for invalid condition %q", "!= 'haha'")
	}
	if !strings.Contains(err.Error(), "missing condition") {
		t.Fatalf("unexpected exception for invalid condition %q.  Expected %q, found %q", "!= 'haha'", "missing condition", err)
	}

	c, rest, err = whereclause.ParseCondition("a 'haha'")
	if err == nil {
		t.Fatalf("expected exception for invalid condition %q", "a 'haha'")
	}
	if !strings.Contains(err.Error(), "no operator found") {
		t.Fatalf("unexpected exception for invalid condition %q.  Expected %q, found %q", "a 'haha'", "no operator found", err)
	}

	c, rest, err = whereclause.ParseCondition("a != ")
	if err == nil {
		t.Fatalf("expected exception for invalid condition %q", "a != ")
	}
	if !strings.Contains(err.Error(), "missing condition value") {
		t.Fatalf("unexpected exception for invalid condition %q.  Expected %q, found %q", "a != ", "missing condition value", err)
	}
}

func TestCondition_Compare(t *testing.T) {
	val := "haha"
	c, _, err := whereclause.ParseCondition("A = '" + val + "'")
	if err != nil {
		t.Fatalf("unexpected exception for condition  %v", err)
	}
	expected := minisql.Values{"A": &val}
	if !c.Compare(expected) {
		t.Fatalf("unexpected false comapre with true values provided")
	}
	val = "hoho" // note: this updates the value inside the expected Values, as its a pointer.
	if c.Compare(expected) {
		t.Fatalf("unexpected true comapre with false values provided")
	}
	c, _, err = whereclause.ParseCondition("A != '" + val + "'")
	if err != nil {
		t.Fatalf("unexpected exception for condition  %v", err)
	}
	if c.Compare(expected) {
		t.Fatalf("unexpected true when comapring with != true values")
	}
	val = "something else"
	if !c.Compare(expected) {
		t.Fatalf("unexpected false when comapring with != false values")
	}

	c, _, err = whereclause.ParseCondition("A = NULL")
	if err != nil {
		t.Fatalf("unexpected exception for condition  %v", err)
	}
	expected = minisql.Values{"A": nil, "B": nil}
	if !c.Compare(expected) {
		t.Fatalf("unexpected false when comapring with = NULL")
	}
	c, _, err = whereclause.ParseCondition("A != NULL")
	if err != nil {
		t.Fatalf("unexpected exception for condition  %v", err)
	}
	if c.Compare(expected) {
		t.Fatalf("unexpected true when comapring with != NuLL")
	}

	c, _, err = whereclause.ParseCondition("C != 1")
	if err != nil {
		t.Fatalf("unexpected exception for condition  %v", err)
	}
	if c.Compare(expected) {
		t.Fatalf("unexpected true when comapring with missing value")
	}
	expected["C"] = nil
	if !c.Compare(expected) {
		t.Fatalf("unexpected false when comapring != 1 with null value")
	}
	a := "A"
	expected["C"] = &a
	if !c.Compare(expected) {
		t.Fatalf("unexpected false when comapring != 1 with value 'A'")
	}
	a = "1"
	if c.Compare(expected) {
		t.Fatalf("unexpected true when comapring != 1 with value '1'")
	}

}
