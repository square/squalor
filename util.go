package squalor

import (
	"errors"
	"fmt"
	"strings"
)

func combineErrors(errs ...error) error {
	var messages []string

	for _, err := range errs {
		messages = append(messages, err.Error())
	}

	return fmt.Errorf(strings.Join(messages, "\n"))
}

func recoveryToError(r interface{}) error {
	var err error

	switch value := r.(type) {
	case string:
		err = errors.New(value)
	case error:
		err = value
	default:
		err = errors.New("unknown panic")
	}

	return err
}

// Rune-based string truncation with ellipsis with reasonable support for multibyte characters.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) < n {
		return s
	}

	offsets := make([]int, 0, n+1)
	for i := range s {
		offsets = append(offsets, i)
		if len(offsets) == n+1 {
			break
		}
	}
	if len(offsets) <= n {
		return s
	}
	return s[:offsets[n-1]] + "…"
}
