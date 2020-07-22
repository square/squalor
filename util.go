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
