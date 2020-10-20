package squalor

import (
	"fmt"
	"github.com/go-mysql/conn"
	"github.com/go-sql-driver/mysql"
	"net"
	"time"
)

var sleeper = time.Sleep

type Retryable interface {
	Retry(fn func() error) error
}

type NoOpConfiguration struct{}

func (n *NoOpConfiguration) Retry(fn func() error) error {
	return fn()
}

type RetryConfiguration struct {
	Retries                     int
	SleepBetweenRetriesInMillis int64
	RetryableExceptions         map[error]bool
}

var BasicRetryConfiguration = RetryConfiguration{
	3,
	300,
	map[error]bool{conn.ErrReadOnly: true, conn.ErrConnLost: true, conn.ErrConnCannotConnect: true, conn.ErrTimeout: true, mysql.ErrInvalidConn: true}}

func (retryConfig *RetryConfiguration) Retry(fn func() error) error {
	var err error
	for i := 0; i < retryConfig.Retries; i++ {
		err = fn()
		if err == nil {
			return nil
		} else if isRetryable(err, retryConfig.RetryableExceptions) {
			// so we don't sleep unnecessarily on the last retry.
			if i < retryConfig.Retries-1 {
				sleeper(time.Duration(retryConfig.SleepBetweenRetriesInMillis))
			}
		} else {
			return err
		}
	}

	return retriesError(retryConfig.Retries, err)
}

func isRetryable(err error, retryableExceptions map[error]bool) bool {
	if opErr, ok := err.(*net.OpError); ok {
		return opErr.Timeout() || opErr.Temporary()
	}

	if myErr, ok := err.(*mysql.MySQLError); ok {
		// https://dev.mysql.com/doc/refman/5.5/en/server-error-reference.html#error_er_option_prevents_statement
		// This is to detect the following error: 'Error 1290: The MySQL server
		// is running with the --read-only option so it cannot execute this statement'
		// Note that the server can return this error in other cases as well.
		return myErr.Number == 1290
	}

	return retryableExceptions[err]
}

func retriesError(retries int, err error) error {
	return fmt.Errorf("Retries Error: exhausted %d retries and still got error: %v ", retries, err)
}
