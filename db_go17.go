// Copyright 2017 Square Inc.
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

// +build !go1.8

package squalor

import (
	"database/sql"

	"golang.org/x/net/context"
)

// executor exposes the sql.DB and sql.Tx functions so that it can be used
// on internal functions that need to be agnostic to the underlying object.
type executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func exec(_ context.Context, ex executor, query string, args ...interface{}) (sql.Result, error) {
	return ex.Exec(query, args...)
}

func query(_ context.Context, ex executor, query string, args ...interface{}) (*sql.Rows, error) {
	return ex.Query(query, args...)
}

func begin(db *DB) (*sql.Tx, error) {
	return db.DB.Begin()
}
