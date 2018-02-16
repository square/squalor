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

// +build go1.8

package squalor

import (
	"context"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestExecutorContextTimeout(t *testing.T) {
	db := makeTestDB(t, usersDDL)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if tx.Context() == nil {
		t.Fatal("Expected default non-nil Context on Tx")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	tx = tx.WithContext(ctx).(*Tx)

	// This query should be canceled.
	if _, err = tx.Query("SELECT SLEEP(1)"); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	// This query should immediately err because the context has already exceeded
	// the timeout.
	if _, err = tx.Query("SELECT 1"); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	if err = tx.Commit(); err != mysql.ErrInvalidConn {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestExecutorContextNoTimeout(t *testing.T) {
	db := makeTestDB(t, usersDDL)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if tx.Context() == nil {
		t.Fatal("Expected default non-nil Context on TX")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tx = tx.WithContext(ctx).(*Tx)

	// This query should not err.
	if _, err := tx.Query("SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestQueryContextTimeout(t *testing.T) {
	db := makeTestDB(t, usersDDL)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This query should be canceled.
	if _, err := db.QueryContext(ctx, "SELECT SLEEP(1)"); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	// This query should immediately err because the context has already exceeded
	// the timeout.
	if _, err := db.QueryContext(ctx, "SELECT 1"); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestQueryContextNoTimeout(t *testing.T) {
	db := makeTestDB(t, usersDDL)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// This query should not err.
	if _, err := db.QueryContext(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}
