package whereclause_test

import (
	"eurozulu/miniSQL/minisql"
	"eurozulu/miniSQL/queries/whereclause"
	"fmt"
	"strings"
	"testing"
)

func TestOperator_SplitOperator(t *testing.T) {
	test := []string{"one", "=", "1"}
	if err := testSplit(test); err != nil {
		t.Fatalf("SplitOperator failed with expression %q  %v", strings.Join(test, ""), err)
	}

	test = []string{"one ", "!=", " 1"}
	if err := testSplit(test); err != nil {
		t.Fatalf("SplitOperator failed with expression %q  %v", strings.Join(test, ""), err)
	}
	test = []string{"one", "<>", "1"}
	if err := testSplit(test); err != nil {
		t.Fatalf("SplitOperator failed with expression %q  %v", strings.Join(test, ""), err)
	}
	test = []string{"one", "LIKE", "'ha ha'"}
	if err := testSplit(test); err != nil {
		t.Fatalf("SplitOperator failed with expression %q  %v", strings.Join(test, ""), err)
	}

	test = []string{"", "=", "1"}
	if err := testSplit(test); err != nil {
		t.Fatalf("SplitOperator unexpected exception for missing name in %q", strings.Join(test, ""))
	}

	test = []string{"one", ".", "1"}
	k, o, v := whereclause.SplitOperator(strings.Join(test, ""))
	if k != strings.Join(test, "") {
		t.Fatalf("unexpoected value on non operator epected %q, found %q", strings.Join(test, ""), k)
	}
	if o != whereclause.OP_UNKNOWN {
		t.Fatalf("unexpoected value on non operator epected %q, found %q", whereclause.OP_UNKNOWN, o)
	}
	if v != "" {
		t.Fatalf("unexpoected value on non operator epected empty string, found %q", v)
	}

	k, o, v = whereclause.SplitOperator("")
	if k != "" {
		t.Fatalf("unexpoected value on non operator epected emptry string, found %q", k)
	}
	if o != whereclause.OP_UNKNOWN {
		t.Fatalf("unexpoected value on non operator epected %q, found %q", whereclause.OP_UNKNOWN, o)
	}
	if v != "" {
		t.Fatalf("unexpoected value on non operator epected empty string, found %q", v)
	}

}

func TestOperatorAND(t *testing.T) {
	et := &mockExpression{Result: true}
	ef := &mockExpression{Result: false}

	and := whereclause.NewOperatorExpression("AND", et)
	and.SetExpression(et)
	if !and.Compare(nil) {
		t.Fatalf("expected true result from double true AND")
	}
	and.SetExpression(ef)
	if and.Compare(nil) {
		t.Fatalf("expected false result from true/false AND")
	}
	and = whereclause.NewOperatorExpression("AND", ef)
	if and.Compare(nil) {
		t.Fatalf("expected false result from false/false AND")
	}
	and.SetExpression(et)
	if and.Compare(nil) {
		t.Fatalf("expected false result from false/true AND")
	}
}

func TestOperatorOR(t *testing.T) {
	et := &mockExpression{Result: true}
	ef := &mockExpression{Result: false}

	or := whereclause.NewOperatorExpression("OR", et)
	or.SetExpression(et)
	if !or.Compare(nil) {
		t.Fatalf("expected true result from double true OR")
	}
	or.SetExpression(ef)
	if !or.Compare(nil) {
		t.Fatalf("expected false result from true/false OR")
	}
	or = whereclause.NewOperatorExpression("OR", ef)
	or.SetExpression(ef)
	if or.Compare(nil) {
		t.Fatalf("expected false result from false/false OR")
	}
	or.SetExpression(et)
	if !or.Compare(nil) {
		t.Fatalf("expected false result from false/true OR")
	}
}

func testSplit(vals []string) error {
	k, o, v := whereclause.SplitOperator(strings.Join(vals, ""))
	if k != strings.TrimSpace(vals[0]) {
		return fmt.Errorf("unexpected column name. expected %q, found %q", vals[0], k)
	}
	if string(o) != strings.TrimSpace(vals[1]) {
		return fmt.Errorf("unexpected operator. expected %q, found %q", vals[1], o)
	}
	if v != strings.TrimSpace(vals[2]) {
		return fmt.Errorf("unexpected value. expected %q, found %q", vals[2], v)
	}
	return nil
}

func TestOperatorNOT(t *testing.T) {
	et := &mockExpression{Result: true}
	ef := &mockExpression{Result: false}

	not := whereclause.NewNOTOperatorExpression("NOT")
	not.SetExpression(et)
	if not.Compare(nil) {
		t.Fatalf("expected false result from True NOT")
	}
	not.SetExpression(ef)
	if !not.Compare(nil) {
		t.Fatalf("expected true result from false NOT")
	}

}

type mockExpression struct {
	Result  bool
	Columns []string
}

func (m mockExpression) Compare(_ minisql.Values) bool {
	return m.Result
}

func (m mockExpression) ColumnNames() []string {
	return m.Columns
}
