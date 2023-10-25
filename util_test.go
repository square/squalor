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

func TestTruncate(t *testing.T) {
	testCases := []struct {
		input, expected string
		n               int
	}{
		{
			"😀😎🌎😀😎🌎", "😀😎🌎😀😎🌎", 7,
		},
		{
			"😀😎🌎😀😎🌎", "😀😎🌎😀😎🌎", 6,
		},
		{
			"😀😎🌎😀😎🌎", "😀😎🌎😀…", 5,
		},
		{
			"😀😎🌎😀😎🌎", "😀😎🌎…", 4,
		},
		{
			"😀😎🌎😀😎🌎", "😀😎…", 3,
		},
		{
			"😀😎🌎😀😎🌎", "😀…", 2,
		},
		{
			"😀😎🌎😀😎🌎", "…", 1,
		},
		{
			"😀😎🌎😀😎🌎", "", 0,
		},
	}

	for _, tc := range testCases {
		actual := truncate(tc.input, tc.n)
		if actual != tc.expected {
			t.Fatalf("Expected %q got %q (n=%d)", tc.expected, actual, tc.n)
		}
	}
}
