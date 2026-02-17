package cmd

import (
	"strings"

	"github.com/couchbaselabs/gorgon/src/gorgon/wildcard"
)

// Filter is a struct that contains the match and exclude patterns for the filter.
type Filter struct {
	match   []wildcard.Matcher
	exclude []wildcard.Matcher
}

// MakeFilter is a function that creates a filter object from the match and exclude patterns.
func MakeFilter(match, exclude string) (filter Filter) {
	for _, p := range strings.Split(match, "|") {
		filter.match = append(filter.match, wildcard.Compile(p))
	}
	if len(exclude) != 0 {
		for _, p := range strings.Split(exclude, "|") {
			filter.exclude = append(filter.exclude, wildcard.Compile(p))
		}
	}
	return
}

// Match is a method that checks if the subject matches the filter.
func (filter Filter) Match(subject string) bool {
	matched := false
	for _, m := range filter.match {
		if m.Match(subject) {
			matched = true
			break
		}
	}
	if !matched {
		return false
	}
	for _, m := range filter.exclude {
		if m.Match(subject) {
			return false
		}
	}
	return true
}
