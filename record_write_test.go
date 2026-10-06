// Copyright 2026 Block, Inc.
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
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type rowCountExecutor struct {
	*recordingExecutor
	rows []int64
}

func (r *rowCountExecutor) ExecContext(ctx context.Context, stmt interface{}, args ...interface{}) (sql.Result, error) {
	if _, err := r.recordingExecutor.ExecContext(ctx, stmt, args...); err != nil {
		return nil, err
	}
	return driver.RowsAffected(r.rows[len(r.exec)-1]), nil
}

type rowCountRecord struct {
	Group int `db:"group_id"`
	ID    int `db:"id"`
	Value int `db:"value"`

	postUpdates int
	postDeletes int
}

func (r *rowCountRecord) PostUpdate(Executor) error {
	r.postUpdates++
	return nil
}

func (r *rowCountRecord) PostDelete(Executor) error {
	r.postDeletes++
	return nil
}

func newRowCountDB(t *testing.T, options ...DBOption) *DB {
	t.Helper()
	db := newTestStatementsDB(t, options...)
	table := NewTable("row_count", rowCountRecord{})
	table.PrimaryKey = &Key{
		Name:    "PRIMARY",
		Primary: true,
		Unique:  true,
		Columns: []*Column{table.ColumnMap["group_id"], table.ColumnMap["id"]},
	}
	modelT := reflect.TypeOf(rowCountRecord{})
	model, err := newModel(db, modelT, *table)
	if err != nil {
		t.Fatal(err)
	}
	db.models[modelT] = model
	db.mappings[modelT] = model.fields
	return db
}

func TestUpdateRecordRowCounts(t *testing.T) {
	testCases := []struct {
		name      string
		rows      []int64
		wantCount int64
		wantExec  int
		wantHooks int
		wantError string
	}{
		{name: "empty"},
		{name: "no rows", rows: []int64{0}, wantExec: 1, wantHooks: 1},
		{name: "one row", rows: []int64{1}, wantCount: 1, wantExec: 1, wantHooks: 1},
		{name: "fewer rows than records", rows: []int64{0, 1}, wantCount: 1, wantExec: 2, wantHooks: 2},
		{name: "too many rows", rows: []int64{2}, wantCount: -1, wantExec: 1, wantError: "affected 2 rows, expected at most 1"},
		{name: "overrun below total record count", rows: []int64{0, 2, 0}, wantCount: -1, wantExec: 2, wantHooks: 1, wantError: "affected 2 rows, expected at most 1"},
		{name: "stop before next record", rows: []int64{2, 1}, wantCount: -1, wantExec: 1, wantError: "affected 2 rows, expected at most 1"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := newRowCountDB(t, ValidateRecordWriteRowCounts(true))
			exec := &rowCountExecutor{recordingExecutor: &recordingExecutor{DB: db}, rows: tc.rows}
			list := make([]interface{}, len(tc.rows))
			for i := range list {
				list[i] = &rowCountRecord{Group: 1, ID: i + 1}
			}
			count, err := updateObjects(context.Background(), db, exec, list)
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "update on table \"row_count\"") || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("expected update row-count error containing %q, got %v", tc.wantError, err)
			}
			if count != tc.wantCount || len(exec.exec) != tc.wantExec {
				t.Fatalf("got count %d and %d statements; want %d and %d", count, len(exec.exec), tc.wantCount, tc.wantExec)
			}
			for i, obj := range list {
				wantHooks := 0
				if i < tc.wantHooks {
					wantHooks = 1
				}
				if got := obj.(*rowCountRecord).postUpdates; got != wantHooks {
					t.Errorf("record %d ran %d post-update hooks; want %d", i, got, wantHooks)
				}
			}
		})
	}
}

func TestUpdateRecordRowCountsOptlock(t *testing.T) {
	for _, rows := range []int64{0, 1, 2} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			db := newTestStatementsDB(t, ValidateRecordWriteRowCounts(true))
			exec := &rowCountExecutor{recordingExecutor: &recordingExecutor{DB: db}, rows: []int64{rows}}
			record := &singleColOptlock{A: 1, B: 2, V: 3}
			count, err := updateObjects(context.Background(), db, exec, []interface{}{record})
			wantVersion := 3
			switch rows {
			case 0:
				if err != ErrConcurrentModificationDetected || count != -1 {
					t.Fatalf("expected concurrent modification error, got count %d and %v", count, err)
				}
			case 1:
				if err != nil || count != 1 {
					t.Fatalf("expected successful update, got count %d and %v", count, err)
				}
				wantVersion++
			case 2:
				if err == nil || !strings.Contains(err.Error(), "affected 2 rows, expected at most 1") || count != -1 {
					t.Fatalf("expected row-count error, got count %d and %v", count, err)
				}
			}
			if record.V != wantVersion {
				t.Fatalf("got version %d, want %d", record.V, wantVersion)
			}
		})
	}
}

func TestDeleteRecordRowCounts(t *testing.T) {
	testCases := []struct {
		name      string
		keys      [][2]int
		rows      []int64
		wantCount int64
		wantExec  int
		wantHooks int
		wantError string
	}{
		{name: "empty"},
		{name: "no rows", keys: [][2]int{{1, 1}}, rows: []int64{0}, wantExec: 1, wantHooks: 1},
		{name: "fewer rows than records", keys: [][2]int{{1, 1}, {1, 2}, {1, 3}}, rows: []int64{1}, wantCount: 1, wantExec: 1, wantHooks: 3},
		{name: "batch limit", keys: [][2]int{{1, 1}, {1, 2}, {1, 3}}, rows: []int64{3}, wantCount: 3, wantExec: 1, wantHooks: 3},
		{name: "duplicate keys", keys: [][2]int{{1, 1}, {1, 1}}, rows: []int64{1}, wantCount: 1, wantExec: 1, wantHooks: 2},
		{name: "multiple batches", keys: [][2]int{{1, 1}, {1, 2}, {2, 1}}, rows: []int64{2, 1}, wantCount: 3, wantExec: 2, wantHooks: 3},
		{name: "too many rows", keys: [][2]int{{1, 1}, {1, 2}}, rows: []int64{3}, wantCount: -1, wantExec: 1, wantError: "affected 3 rows, expected at most 2"},
		{name: "overrun below total record count", keys: [][2]int{{1, 1}, {1, 2}, {2, 1}, {3, 1}}, rows: []int64{0, 2, 1}, wantCount: -1, wantExec: 2, wantHooks: 2, wantError: "affected 2 rows, expected at most 1"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := newRowCountDB(t, ValidateRecordWriteRowCounts(true))
			exec := &rowCountExecutor{recordingExecutor: &recordingExecutor{DB: db}, rows: tc.rows}
			list := make([]interface{}, len(tc.keys))
			for i, key := range tc.keys {
				list[i] = &rowCountRecord{Group: key[0], ID: key[1]}
			}
			count, err := deleteObjects(context.Background(), db, exec, list)
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "delete on table \"row_count\"") || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("expected delete row-count error containing %q, got %v", tc.wantError, err)
			}
			if count != tc.wantCount || len(exec.exec) != tc.wantExec {
				t.Fatalf("got count %d and %d statements; want %d and %d", count, len(exec.exec), tc.wantCount, tc.wantExec)
			}
			for i, obj := range list {
				wantHooks := 0
				if i < tc.wantHooks {
					wantHooks = 1
				}
				if got := obj.(*rowCountRecord).postDeletes; got != wantHooks {
					t.Errorf("record %d ran %d post-delete hooks; want %d", i, got, wantHooks)
				}
			}
		})
	}
}

// A deliberately mismatched model: MySQL can coerce the VARCHAR primary key
// to a number when compared to the numeric literal generated from ID.
type rowCountCoercionRecord struct {
	ID    int `db:"id"`
	Value int `db:"value"`
}

func TestRecordWriteRowCountValidationOptions(t *testing.T) {
	options := []struct {
		name    string
		options []DBOption
		enabled bool
	}{
		{name: "default"},
		{name: "disabled", options: []DBOption{ValidateRecordWriteRowCounts(false)}},
		{name: "enabled", options: []DBOption{ValidateRecordWriteRowCounts(true)}, enabled: true},
		{name: "last option wins", options: []DBOption{ValidateRecordWriteRowCounts(true), ValidateRecordWriteRowCounts(false)}},
	}
	for _, tc := range options {
		for _, transaction := range []bool{false, true} {
			for _, operation := range []string{"update", "delete"} {
				t.Run(fmt.Sprintf("%s/transaction=%t/%s", tc.name, transaction, operation), func(t *testing.T) {
					db := makeTestDBWithOptions(t, tc.options,
						"CREATE TABLE row_count_coercion (id VARCHAR(8) PRIMARY KEY, value INT NOT NULL) ENGINE=InnoDB",
						"INSERT INTO row_count_coercion VALUES ('1', 0), ('01', 0)",
					)
					defer db.Close()
					db.MustBindModel("row_count_coercion", rowCountCoercionRecord{})
					var exec Executor = db.WithContext(context.Background())
					if transaction {
						tx, err := db.Begin()
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Rollback()
						exec = tx.WithContext(context.Background())
					}
					record := &rowCountCoercionRecord{ID: 1, Value: 1}
					var count int64
					var err error
					if operation == "update" {
						count, err = exec.Update(record)
					} else {
						count, err = exec.DeleteContext(context.Background(), record)
					}
					if tc.enabled {
						if err == nil || !strings.Contains(err.Error(), "affected 2 rows, expected at most 1") || count != -1 {
							t.Fatalf("expected row-count error, got count %d and %v", count, err)
						}
					} else if err != nil || count != 2 {
						t.Fatalf("expected original behavior, got count %d and %v", count, err)
					}
				})
			}
		}
	}
}

func TestUpdateRecordRowCountsOptlockDefault(t *testing.T) {
	db := newTestStatementsDB(t)
	exec := &rowCountExecutor{recordingExecutor: &recordingExecutor{DB: db}, rows: []int64{2}}
	record := &singleColOptlock{A: 1, B: 2, V: 3}
	count, err := updateObjects(context.Background(), db, exec, []interface{}{record})
	if err != ErrConcurrentModificationDetected || count != -1 || record.V != 3 {
		t.Fatalf("expected original optimistic-lock behavior, got count %d, error %v and version %d", count, err, record.V)
	}
}
