package squalor

import (
	"errors"
	"strings"
	"testing"
)

func TestCombineErrors(t *testing.T) {
	errs := []error{
		errors.New("error 1"),
		errors.New("error 2"),
	}

	expectedErr := strings.Join([]string{"error 1", "error 2"}, "\n")

	if err := combineErrors(errs...); err == nil || err.Error() != expectedErr {
		t.Fatalf("Expected: %v, got: %v", expectedErr, err)
	}
}

func TestRecoveryToError(t *testing.T) {
	expectedErr := errors.New("panic")

	if err := recoveryToError(errors.New("panic")); err == nil || err.Error() != expectedErr.Error() {
		t.Fatalf("Expected: %v, got %v", expectedErr, err)
	}

	if err := recoveryToError("panic"); err == nil || err.Error() != expectedErr.Error() {
		t.Fatalf("Expected: %v, got %v", expectedErr, err)
	}

	expectedErr = errors.New("unknown panic")

	if err := recoveryToError(nil); err == nil || err.Error() != expectedErr.Error() {
		t.Fatalf("Expected: %v, got %v", expectedErr, err)
	}
}
