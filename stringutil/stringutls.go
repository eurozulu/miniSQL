package stringutil

import (
	"strings"
)

func Contains(s string, ss []string) bool {
	return IndexOf(s, ss) >= 0
}

func IndexOf(s string, ss []string) int {
	for i, sz := range ss {
		if sz == s {
			return i
		}
	}
	return -1
}

func FirstWord(s string) (w string, rest string) {
	ss := strings.SplitN(s, " ", 2)
	return ss[0], strings.TrimSpace(strings.Join(ss[1:], ""))
}

func LastWord(s string) (before string, w string) {
	i := strings.LastIndex(s, " ")
	if i < 0 || i == len(s)-1 {
		return before, ""
	}
	return strings.TrimSpace(s[:i]), s[i+1:]
}

// BracketedString finds the first string enclosed in brackets.
// if given string starts with a bracket, the result will contain all the string enclosed between that opening bracket and its corrisponding closing bracket.
// Any string after the closing bracket is returned in rest.
// if given doesn't start with a bracket, all of s is returned in rest.
// e.g. "(hello)" = "hello", ""
//      "(hello) world" = "hello", " world"
//      "hello world" = "", "hello world"
//      "(hello (world))" = "hello (world)", ""
//      "(hello (world)) (Goodbye)" = "hello (world)", " (Goodbye)"
// Unmatched brackets (unclosed, or unpaired brackets results in whole string returned as rest.
func BracketedString(s string) (bracketed, rest string) {
	if !strings.HasPrefix(s, "(") {
		return "", s
	}
	var count int
	var index = -1
	for i, b := range []byte(s) {
		switch b {
		case '(':
			count++
		case ')':
			count--
			if count == 0 {
				index = i
			}
		}
		if index >= 0 {
			break
		}
	}
	if count != 0 || index < 0 {
		return "", s
	}
	bracketed = strings.Trim(s[:index], "()")
	if index+1 < len(s) {
		rest = s[index+1:]
	}
	return bracketed, rest
}

func SplitTrim(s string, sep string) []string {
	ss := strings.Split(s, sep)
	for i, sz := range ss {
		ss[i] = strings.TrimSpace(sz)
	}
	return ss
}

// SplitIgnoreQuoted splits the given string with the given seperator, ignoring any poart of the string enclosed in quotes.
// Quotes may be double or single, which ever appears first in the string.
// e.g. SplitIgnoreQuoted("one two "three and four" five", " ") returns ["one", "two", ""three and four"", "five"]
func SplitIgnoreQuoted(s string, sep string) []string {
	i := firstQuoteIndex(s)
	if i < 0 {
		// no quotes found, split as normal
		return strings.Split(s, sep)
	}
	quote := s[i : i+1]
	var result []string
	// first break into segments of unquoted and quoted
	ss := strings.Split(s, quote)
	var inQuote bool
	for _, si := range ss {
		if inQuote {
			//  inside quote, add quoted section to previous entry (always there)
			li := len(result) - 1
			result[li] = strings.Join([]string{result[li], quote, si, quote}, "")
		} else {
			// outside quote, split as usual
			result = append(result, strings.Split(si, sep)...)
		}
		inQuote = !inQuote
	}
	/*
		if !inQuote {
			// inQuote inverse of last segment. if still !inQuote, mismatched quotes
			log.Printf("%s has mismatched quote!")
		}
	*/
	return result
}

func Unquote(s string) string {
	i := firstQuoteIndex(s)
	if i != 0 {
		return s
	}
	return strings.Trim(s, s[0:1])
}

func firstQuoteIndex(s string) int {
	dqi := strings.Index(s, "\"")
	sqi := strings.Index(s, "'")
	if sqi < 0 {
		return dqi
	}
	if dqi < 0 {
		return sqi
	}
	if dqi < sqi {
		return dqi
	}
	return sqi
}

func UniqueStrings(ss []string) []string {
	var result []string
	for _, s := range ss {
		if IndexOf(s, result) >= 0 {
			continue
		}
		result = append(result, s)
	}
	return result
}
