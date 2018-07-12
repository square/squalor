// Copyright 2015 Square Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package squalor

import (
	"log"
	"time"

	"golang.org/x/net/context"
)

// QueryLogger defines an interface for query loggers.
type QueryLogger interface {
	// Log is called on completion of a query with a Serializer for the
	// query, the Executor it was called on, the execution time of the query
	// and an error if one occurred.
	//
	// The Executor may be used to trace queries within a transaction because
	// queries in the same transaction will use the same executor.
	Log(ctx context.Context, query Serializer, exec Executor, executionTime time.Duration, err error)
}

// StandardLogger implements the QueryLogger interface and wraps a log.Logger.
type StandardLogger struct {
	*log.Logger
}

func (l *StandardLogger) Log(ctx context.Context, query Serializer, exec Executor, executionTime time.Duration, err error) {
	queryStr, serializeErr := Serialize(query)
	if serializeErr != nil {
		return
	}

	if err != nil {
		l.Printf("[%v] %s - `%s` - %s\n", exec, executionTime, queryStr, err)
	} else {
		l.Printf("[%v] %s - `%s`\n", exec, executionTime, queryStr)
	}
}
