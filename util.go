package squalor

import (
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
