package squalor

import (
	"github.com/go-mysql/conn"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

func TestRetryWithParameters(t *testing.T) {
	testCases := []struct {
		name               string
		retryConfiguration RetryConfiguration
		results            []error
		expectedCalls      int
		expectedSleptTime  time.Duration
		expectedError      error
	}{
		{
			name:               "No Error",
			retryConfiguration: DefaultRetryConfiguration,
			results:            []error{nil},
			expectedCalls:      1,
			expectedSleptTime:  0,
			expectedError:      nil,
		},
		{
			name:               "Retriable Error and Success after first retry",
			retryConfiguration: DefaultRetryConfiguration,
			results:            []error{conn.ErrConnLost, nil},
			expectedCalls:      2,
			expectedSleptTime:  time.Duration(DefaultRetryConfiguration.SleepBetweenRetriesInMillis),
			expectedError:      nil,
		},
		{
			name:               "Retriable Error and Success after second retry",
			retryConfiguration: DefaultRetryConfiguration,
			results:            []error{conn.ErrConnLost, conn.ErrReadOnly, nil},
			expectedCalls:      3,
			expectedSleptTime:  2 * time.Duration(DefaultRetryConfiguration.SleepBetweenRetriesInMillis),
			expectedError:      nil,
		},
		{
			name:               "Retriable Error and exhausted retries",
			retryConfiguration: DefaultRetryConfiguration,
			results:            []error{conn.ErrConnLost, conn.ErrReadOnly, conn.ErrConnLost},
			expectedCalls:      3,
			expectedSleptTime:  2 * time.Duration(DefaultRetryConfiguration.SleepBetweenRetriesInMillis),
			expectedError:      retriesError(3, conn.ErrConnLost),
		},
		{
			name:               "Retriable Error and Non Retriable",
			retryConfiguration: DefaultRetryConfiguration,
			results:            []error{conn.ErrConnLost, conn.ErrDupeKey},
			expectedCalls:      2,
			expectedSleptTime:  time.Duration(DefaultRetryConfiguration.SleepBetweenRetriesInMillis),
			expectedError:      conn.ErrDupeKey,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sleptTime := time.Duration(0)
			sleeper = func(d time.Duration) {
				sleptTime = sleptTime + d
			}

			calls := 0
			fn := func() error {
				value := tc.results[calls]
				calls = calls + 1
				return value
			}

			err := Retry(tc.retryConfiguration, fn)
			assert.Equal(t, tc.expectedCalls, calls)
			assert.Equal(t, tc.expectedSleptTime, sleptTime)
			assert.Equal(t, tc.expectedError, err)
		})
	}
}

type mockTimeoutErr struct{}

func (e *mockTimeoutErr) Error() string { return "i/o timeout" }
func (e *mockTimeoutErr) Timeout() bool { return true }

type mockTemporaryErr struct{}

func (e *mockTemporaryErr) Error() string   { return "i/o temporary" }
func (e *mockTemporaryErr) Temporary() bool { return true }

func TestIsIsRetriable(t *testing.T) {
	testCases := []struct {
		name            string
		error           error
		retryableErrors map[error]bool
		retriable       bool
	}{
		{
			name:            "ErrReadOnly",
			error:           conn.ErrReadOnly,
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "ErrConnLost",
			error:           conn.ErrConnLost,
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "ErrConnCannotConnect",
			error:           conn.ErrConnCannotConnect,
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "ErrDupeKey",
			error:           conn.ErrDupeKey,
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       false,
		},
		{
			name:            "ErrTimeout",
			error:           conn.ErrTimeout,
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "net.OpError",
			error:           &net.OpError{},
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       false,
		},
		{
			name:            "net.OpError Timeout",
			error:           &net.OpError{Err: &mockTimeoutErr{}},
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "net.OpError Temporary",
			error:           &net.OpError{Err: &mockTemporaryErr{}},
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "mysql.ErrInvalidConn",
			error:           mysql.ErrInvalidConn,
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "mysql.MySQLError with code other than ER_OPTION_PREVENTS_STATEMENT(1290)",
			error:           &mysql.MySQLError{Number: 1200},
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       false,
		},
		{
			// https://dev.mysql.com/doc/refman/5.5/en/server-error-reference.html#error_er_option_prevents_statement
			name:            "mysql.MySQLError with code ER_OPTION_PREVENTS_STATEMENT(1290)",
			error:           &mysql.MySQLError{Number: 1290},
			retryableErrors: DefaultRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsRetryable(tc.error, tc.retryableErrors)
			assert.Equal(t, tc.retriable, result)
		})
	}
}
