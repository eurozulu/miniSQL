package whereclause

import (
	"log"
	"math"
	"regexp"
	"strings"
)

type Operator string

const (
	OP_GREATER_OR_EQUAL Operator = ">="
	OP_LESS_OR_EQUAL    Operator = "<="
	OP_NOT_EQUAL        Operator = "<>"
	OP_NOT_EQUAL_ALT    Operator = "!="
	OP_EQUAL            Operator = "="
	OP_GREATER          Operator = ">"
	OP_LESS             Operator = "<"
	OP_LIKE             Operator = "LIKE"
	OP_UNKNOWN          Operator = ""
)

// operators is a slice of all the operators
var operators = []Operator{
	OP_EQUAL,
	OP_GREATER,
	OP_LESS,
	OP_GREATER_OR_EQUAL,
	OP_LESS_OR_EQUAL,
	OP_NOT_EQUAL,
	OP_NOT_EQUAL_ALT,
	OP_LIKE,
}

// SplitOperator splits the given string into three parts, using the first Operator found as a delimiter.
// returns the string preceeding the found Operator, followed by the Operator itself, and then any string following the Operator.
// returned strings are trimmed of any space.
// If no Operator is found returns <given string>, OP_UNKNOWN, ""
func SplitOperator(s string) (string, Operator, string) {
	i, op := firstOperatorIndex(s)
	if i < 0 {
		return s, OP_UNKNOWN, ""
	}
	b4 := strings.TrimSpace(s[:i])
	i += len(op) // skip past the Operator
	var rest string
	if i < len(s) {
		rest = strings.TrimSpace(s[i:])
	}
	return b4, op, rest
}

func (op Operator) Compare(v1, v2 *string) bool {
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
		if bothNull {
			return false
		}
		if eitherNull {
			return v2 == nil
		}
		return strings.Compare(*v1, *v2) > 0

	case OP_GREATER_OR_EQUAL:
		if bothNull {
			return true
		}
		if eitherNull {
			return v2 == nil
		}
		return *v1 == *v2 || strings.Compare(*v1, *v2) > 0

	case OP_LESS:
		if bothNull {
			return false
		}
		if eitherNull {
			return v1 == nil
		}
		return strings.Compare(*v1, *v2) < 0

	case OP_LESS_OR_EQUAL:
		if bothNull {
			return true
		}
		if eitherNull {
			return v1 == nil
		}
		return *v1 == *v2 || strings.Compare(*v1, *v2) < 0

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
		log.Printf("%q is not a known Operator\n", op)
		return false
	}
}

func firstOperatorIndex(s string) (index int, op Operator) {
	index = math.MaxInt
	op = OP_UNKNOWN
	for _, o := range operators {
		i := strings.Index(s, string(o))
		if i < 0 {
			continue
		}
		// found before any previous or same index, but longer Operator
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
