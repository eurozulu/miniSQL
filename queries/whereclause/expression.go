package whereclause

import (
	"eurozulu/miniSQL/minisql"
	"eurozulu/miniSQL/stringutil"
	"fmt"
	"strings"
)

const (
	AND = "AND"
	OR  = "OR"
	NOT = "NOT"
)

// An Expression is something that can be presented with some values and result in a boolean outcome
type Expression interface {
	// Match will compare the named value in the expression with a value in the given values.
	Compare(values minisql.Values) bool

	// ColumnNames gets all the Column names used in the expression
	ColumnNames() []string
}

// OperatorExpression are expressions which alter the outcome of other expressions or link two expressions together.
// AND, NOT and OR are the three operator expressions (Not to be confused with condition operators such as =, <, >)
type OperatorExpression interface {
	Expression
	// SetExpression sets the expression this operator should act on
	SetExpression(ex Expression)
}

// NotExpression inverts the outcome of another expression.
type NotExpression struct {
	expression Expression
}

func (oe *NotExpression) SetExpression(ex Expression) {
	oe.expression = ex
}

func (oe NotExpression) ColumnNames() []string {
	return stringutil.UniqueStrings(oe.expression.ColumnNames())
}

func (oe NotExpression) Compare(values minisql.Values) bool {
	return !oe.expression.Compare(values)
}

// AndExpression performs an operation on two expressions, resulting in an AND of both results
type AndExpression struct {
	operand    Expression
	expression Expression
}

func (oe *AndExpression) SetExpression(ex Expression) {
	oe.expression = ex
}

func (oe AndExpression) ColumnNames() []string {
	return stringutil.UniqueStrings(append(oe.operand.ColumnNames(), oe.expression.ColumnNames()...))
}

func (oe AndExpression) Compare(values minisql.Values) bool {
	return oe.operand.Compare(values) && oe.expression.Compare(values)
}

// OrExpression performs an operation on two expressions, resulting in an OR of both results
type OrExpression struct {
	operand    Expression
	expression Expression
}

func (oe *OrExpression) SetExpression(ex Expression) {
	oe.expression = ex
}

func (oe OrExpression) ColumnNames() []string {
	return stringutil.UniqueStrings(append(oe.operand.ColumnNames(), oe.expression.ColumnNames()...))
}

func (oe OrExpression) Compare(values minisql.Values) bool {
	return oe.operand.Compare(values) || oe.expression.Compare(values)
}

func NewOperatorExpression(s string, operand Expression) OperatorExpression {
	switch strings.ToUpper(s) {
	case AND:
		return &AndExpression{operand: operand}
	case OR:
		return &OrExpression{operand: operand}
	default:
		return nil
	}
}

func NewNOTOperatorExpression(s string) OperatorExpression {
	if strings.ToUpper(s) == NOT {
		return &NotExpression{}
	}
	return nil
}

// parseNextExpression attempts to parse the first expression from the given string.
// The first expression will be a condition or a bracketed expression
// If expression is preceded with the NOT operator, the expression will be returned wrapped in a NOT OperatorExpression
// any string remaining after the expression is returned along with the parsed expression.
func parseNextExpression(s string) (Expression, string, error) {
	// bracketed string, treat its contents as a single expression
	if strings.HasPrefix(s, "(") {
		bs, rest := stringutil.BracketedString(s)
		ex, err := ParseExpression(bs)
		if err != nil {
			return nil, rest, err
		}
		return ex, rest, nil
	}

	// check if it's a NOT op:
	cmd, rest := stringutil.FirstWord(s)
	not := NewNOTOperatorExpression(cmd)
	if not != nil {
		// parse following as an expression (may be complex, bracketed expression)
		ex, r, err := parseNextExpression(rest)
		if err != nil {
			return nil, rest, err
		}
		not.SetExpression(ex)
		return not, r, nil
	}

	// not a bracket or NOT, treat as a condition <key=value>
	var c Expression
	c, rest, err := ParseCondition(s)
	if err != nil {
		return nil, rest, err
	}
	return c, rest, nil
}

// ParseExpression the given string into an Expression.
func ParseExpression(s string) (Expression, error) {
	// must have at least one expression
	ex, rest, err := parseNextExpression(s)
	if err != nil {
		return nil, err
	}
	rest = strings.TrimSpace(rest)
	// further expressions must be delimited with operators OR or AND
	for len(rest) > 0 {
		// parse joining operator (AND/OR), using previously parsed expression as its operand
		cmd, r := stringutil.FirstWord(rest)
		op := NewOperatorExpression(cmd, ex)
		if op == nil {
			return nil, fmt.Errorf("unexpected %q after expression. Expected 'OR' or 'AND'", cmd)
		}

		// parse the following expression to add to the operator
		e, er, err := parseNextExpression(r)
		if err != nil {
			return nil, err
		}
		op.SetExpression(e)
		ex = op
		rest = strings.TrimSpace(er)
	}
	return ex, nil
}
