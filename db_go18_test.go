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
)

func TestWithContextTimeout(t *testing.T) {
	db := makeTestDB(t, usersDDL)
	defer db.Close()

	if db.GetContext() == nil {
		t.Fatal("Expected default non-nil Context on DB")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	db = db.WithContext(ctx).(*DB)

	// This query should be canceled.
	if _, err := db.Query("SELECT SLEEP(1)"); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	// This query should immediately err because the context has already exceeded
	// the timeout.
	if _, err := db.Query("SELECT 1"); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestWithContextNoTimeout(t *testing.T) {
	db := makeTestDB(t, usersDDL)
	defer db.Close()

	if db.GetContext() == nil {
		t.Fatal("Expected default non-nil Context on DB")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	db = db.WithContext(ctx).(*DB)

	// This query should not err.
	if _, err := db.Query("SELECT 1"); err != nil {
		t.Fatal(err)
	}
}
