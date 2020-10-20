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
		retryConfiguration Retryable
		results            []error
		expectedCalls      int
		expectedSleptTime  time.Duration
		expectedError      error
	}{
		{
			name:               "No Error NoOpRetry",
			retryConfiguration: &NoOpConfiguration{},
			results:            []error{nil},
			expectedCalls:      1,
			expectedSleptTime:  0,
			expectedError:      nil,
		},
		{
			name:               "An Error NoOpRetry not retried.",
			retryConfiguration: &NoOpConfiguration{},
			results:            []error{conn.ErrConnLost, nil},
			expectedCalls:      1,
			expectedSleptTime:  0,
			expectedError:      conn.ErrConnLost,
		},
		{
			name:               "No Error",
			retryConfiguration: &BasicRetryConfiguration,
			results:            []error{nil},
			expectedCalls:      1,
			expectedSleptTime:  0,
			expectedError:      nil,
		},
		{
			name:               "Retriable Error and Success after first retry",
			retryConfiguration: &BasicRetryConfiguration,
			results:            []error{conn.ErrConnLost, nil},
			expectedCalls:      2,
			expectedSleptTime:  time.Duration(BasicRetryConfiguration.SleepBetweenRetriesInMillis),
			expectedError:      nil,
		},
		{
			name:               "Retriable Error and Success after second retry",
			retryConfiguration: &BasicRetryConfiguration,
			results:            []error{conn.ErrConnLost, conn.ErrReadOnly, nil},
			expectedCalls:      3,
			expectedSleptTime:  2 * time.Duration(BasicRetryConfiguration.SleepBetweenRetriesInMillis),
			expectedError:      nil,
		},
		{
			name:               "Retriable Error and exhausted retries",
			retryConfiguration: &BasicRetryConfiguration,
			results:            []error{conn.ErrConnLost, conn.ErrReadOnly, conn.ErrConnLost},
			expectedCalls:      3,
			expectedSleptTime:  2 * time.Duration(BasicRetryConfiguration.SleepBetweenRetriesInMillis),
			expectedError:      retriesError(3, conn.ErrConnLost),
		},
		{
			name:               "Retriable Error and Non Retriable",
			retryConfiguration: &BasicRetryConfiguration,
			results:            []error{conn.ErrConnLost, conn.ErrDupeKey},
			expectedCalls:      2,
			expectedSleptTime:  time.Duration(BasicRetryConfiguration.SleepBetweenRetriesInMillis),
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

			err := tc.retryConfiguration.Retry(fn)
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
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "ErrConnLost",
			error:           conn.ErrConnLost,
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "ErrConnCannotConnect",
			error:           conn.ErrConnCannotConnect,
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "ErrDupeKey",
			error:           conn.ErrDupeKey,
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       false,
		},
		{
			name:            "ErrTimeout",
			error:           conn.ErrTimeout,
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "net.OpError",
			error:           &net.OpError{},
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       false,
		},
		{
			name:            "net.OpError Timeout",
			error:           &net.OpError{Err: &mockTimeoutErr{}},
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "net.OpError Temporary",
			error:           &net.OpError{Err: &mockTemporaryErr{}},
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "mysql.ErrInvalidConn",
			error:           mysql.ErrInvalidConn,
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       true,
		},
		{
			name:            "mysql.MySQLError with code other than ER_OPTION_PREVENTS_STATEMENT(1290)",
			error:           &mysql.MySQLError{Number: 1200},
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
			retriable:       false,
		},
		{
			// https://dev.mysql.com/doc/refman/5.5/en/server-error-reference.html#error_er_option_prevents_statement
			name:            "mysql.MySQLError with code ER_OPTION_PREVENTS_STATEMENT(1290)",
			error:           &mysql.MySQLError{Number: 1290},
			retryableErrors: BasicRetryConfiguration.RetryableExceptions,
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
