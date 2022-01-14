package whereclause

import (
	"eurozulu/tinydb/stringutil"
	"log"
	"math"
	"regexp"
	"strings"
)

type operator string

const (
	OP_GREATER_OR_EQUAL operator = ">="
	OP_LESS_OR_EQUAL    operator = "<="
	OP_NOT_EQUAL        operator = "<>"
	OP_NOT_EQUAL_ALT    operator = "!="
	OP_EQUAL            operator = "="
	OP_GREATER          operator = ">"
	OP_LESS             operator = "<"
	OP_LIKE             operator = "LIKE"
	OP_UNKNOWN          operator = ""
)

// operators is a slice of all the operators
var operators = []operator{
	OP_EQUAL,
	OP_GREATER,
	OP_LESS,
	OP_GREATER_OR_EQUAL,
	OP_LESS_OR_EQUAL,
	OP_NOT_EQUAL,
	OP_NOT_EQUAL_ALT,
	OP_LIKE,
}

// SplitOperator splits the given string into three parts, using the first operator found as a delimiter.
// returns the string preceeding the found operator, followed by the operator itself, and then any string following the operator.
// returned strings are trimmed of any space.
// If no operator is found returns <given string>, OP_UNKNOWN, ""
func SplitOperator(s string) (string, operator, string) {
	i, op := firstOperatorIndex(s)
	if i < 0 {
		return s, OP_UNKNOWN, ""
	}
	b4 := strings.TrimSpace(s[:i])
	i += len(op) // skip past the operator
	var rest string
	if i < len(s) {
		rest = strings.TrimSpace(s[i:])
	}
	return b4, op, rest

}

func (op operator) Compare(v1, v2 *string) bool {
	bothNull := (v1 == nil && v2 == nil)
	eitherNull := (v1 == nil || v2 == nil)
	switch op {
	case OP_EQUAL:
		if bothNull {
			return true
		}
		if eitherNull {
			return false
		}
		return *v1 == *v2

	case OP_GREATER:
		if eitherNull {
			return false
		}
		return stringutil.StringGreaterThat(*v1, *v2)

	case OP_GREATER_OR_EQUAL:
		if bothNull {
			return true
		}
		if eitherNull {
			return false
		}
		return *v1 == *v2 || stringutil.StringGreaterThat(*v1, *v2)

	case OP_LESS:
		if eitherNull {
			return false
		}
		return stringutil.StringLessThat(*v1, *v2)

	case OP_LESS_OR_EQUAL:
		if bothNull {
			return true
		}
		if eitherNull {
			return false
		}
		return *v1 == *v2 || stringutil.StringLessThat(*v1, *v2)

	case OP_NOT_EQUAL, OP_NOT_EQUAL_ALT:
		if bothNull {
			return false
		}
		if eitherNull {
			return true
		}
		return *v1 != *v2

	case OP_LIKE:
		if eitherNull {
			return false
		}
		regx, err := likeToRegex(*v2)
		if err != nil {
			log.Printf("Where LIKE %q invalid.", *v2)
			return false
		}
		return regx.MatchString(*v1)
	default:
		log.Printf("%q is not a known operator\n", op)
		return false
	}
}

func firstOperatorIndex(s string) (index int, op operator) {
	index = math.MaxInt
	op = OP_UNKNOWN
	for _, o := range operators {
		i := strings.Index(s, string(o))
		if i < 0 {
			continue
		}
		// found before any previous or same index, but longer operator
		if i < index || (i == index && len(o) > len(op)) {
			index = i
			op = o
		}
	}
	if index >= len(s) {
		// no OP found
		index = -1
	}
	return index, op
}

func likeToRegex(s string) (*regexp.Regexp, error) {
	rx := strings.Replace(s, "%", ".*", -1)
	rx = strings.Replace(rx, "_", ".", -1)
	rx = strings.Join([]string{"^", rx}, "")
	return regexp.Compile(rx)
}
