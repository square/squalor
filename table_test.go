// Copyright 2014 Square Inc.
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
	"fmt"
	"strings"
	"testing"
)

func mustLoadTable(t *testing.T, name string) *Table {
	db := makeTestDB(t, objectsDDL)
	defer db.Close()

	table, err := LoadTable(db.DB, name)
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func TestLoadTable(t *testing.T) {
	table := mustLoadTable(t, "objects")
	fmt.Printf("%s\n", table)
}

func TestGetKey(t *testing.T) {
	table := mustLoadTable(t, "objects")

	testCases := []struct {
		cols   []string
		result bool
	}{
		{[]string{"user_id", "object_id"}, true},
		{[]string{"USER_ID", "object_id"}, false},
		{[]string{"user_id", "timestamp"}, true},
		{[]string{"timestamp", "user_id"}, false},
	}
	for _, c := range testCases {
		key := table.GetKey(c.cols...)
		if c.result && key == nil {
			t.Errorf("Unable to find key (%s)", strings.Join(c.cols, ","))
		} else if !c.result && key != nil {
			t.Errorf("Expected to not find key, but got %s", key)
		}
	}
}

func TestValidateModel(t *testing.T) {
	table := mustLoadTable(t, "objects")

	type BaseModel struct {
		UserID    int64  `db:"user_id"`
		ID        string `db:"object_id"`
		Timestamp int64  `db:"timestamp"`
	}

	type ObjectModel struct {
		BaseModel
		Value   string `db:"value"`
		Ignored int    `db:"-"`
		ignored int
	}

	if err := table.ValidateModel(ObjectModel{}); err != nil {
		t.Fatal(err)
	}

	// BaseModel doesn't map all of the table's columns.
	if err := table.ValidateModel(BaseModel{}); err == nil {
		t.Fatal("Expected failure, but found success")
	}

	type BadModel struct {
		ObjectModel
		Extra string `db:"extra"`
	}

	// BadModel maps columns that are not in the table.
	if err := table.ValidateModel(BadModel{}); err == nil {
		t.Fatal("Expected failure, but found success")
	}
}
