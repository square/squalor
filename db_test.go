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
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/coopernurse/gorp"
)

const objectsDDL = `
CREATE TABLE objects (
  user_id     BIGINT         NOT NULL,
  object_id   VARBINARY(767) NOT NULL,
  value       BLOB           NULL,
  timestamp   BIGINT         NULL,
  PRIMARY KEY (user_id, object_id),
  INDEX (user_id, timestamp)
)`

const objectsDDLWithUnmappedColumns = `
CREATE TABLE objects (
  user_id     BIGINT         NOT NULL,
  object_id   VARBINARY(767) NOT NULL,
  value       BLOB           NULL,
  timestamp   BIGINT         NULL,
  unmapped    VARBINARY(767) NULL,
  PRIMARY KEY (user_id, object_id),
  INDEX (user_id, timestamp)
)`

const objectsDDLAddUnmappedColumns = `
ALTER TABLE objects ADD COLUMN (
  unmapped VARBINARY(767) NULL
)`

const objectsDDLWithoutPrimaryKey = `
CREATE TABLE objects (
  user_id     BIGINT         NOT NULL,
  object_id   VARBINARY(767) NOT NULL,
  value       BLOB           NULL,
  timestamp   BIGINT         NULL,
  INDEX (user_id, timestamp)
)`

const usersDDL = `
CREATE TABLE users (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL
)`

func TestDB_WithoutUnmappedColumns(t *testing.T) {
	db := makeTestDB(t, objectsDDL)
	defer db.Close()

	testDB(db, t)
}

func TestDB_WithUnmappedColumnsIgnored(t *testing.T) {
	db := makeTestDB(t, objectsDDLWithUnmappedColumns)
	defer db.Close()

	testDB(db, t)
}

func testDB(db *DB, t *testing.T) {
	type BaseModel struct {
		UserID    int64  `db:"user_id"`
		ID        string `db:"object_id"`
		Timestamp *int64 `db:"timestamp"`
	}

	type Object struct {
		BaseModel
		Value []byte `db:"value"`
	}

	items, err := db.BindModel("objects", Object{})
	if err != nil {
		t.Fatal(err)
	}

	i := &Object{}
	if err := db.Get(i, 1, "bar"); err == nil {
		t.Fatalf("Expected error, but found success")
	}

	// Insert.
	i.UserID = 1
	i.ID = "bar"
	i.Value = []byte("hello world")
	// Non-pointer should fail.
	if err := db.Insert(*i); err == nil {
		t.Fatalf("Expected error, but found success")
	}
	// Pointer should succeed.
	if err := db.Insert(i); err != nil {
		t.Fatal(err)
	}
	// Duplicate entry should fail
	if err := db.Insert(i); err == nil {
		t.Fatal("Expected err, but found success")
	}
	j := &Object{}
	// Non-pointer should fail.
	if err := db.Get(*j, 1, "bar"); err == nil {
		t.Fatalf("Expected error, but found success")
	}
	// Pointer should succeed.
	if err := db.Get(j, 1, "bar"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(i, j) {
		t.Fatalf("Expected %+v, but got %+v", i, j)
	}
	if j.Timestamp != nil {
		t.Fatalf("Expected nil, but got %+v", j.Timestamp)
	}

	// Update.
	i.Value = nil
	timestamp := int64(42)
	i.Timestamp = &timestamp
	// Non-pointer should fail.
	if _, err := db.Update(*i); err == nil {
		t.Fatalf("Expected error, but found success")
	}
	// Pointer should succeed.
	if n, err := db.Update(i); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("Expected 1 update, but got %d", n)
	}

	if err := db.Get(j, 1, "bar"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(i, j) {
		t.Fatalf("Expected %+v, but got %+v", i, j)
	}
	if j.Value != nil {
		t.Fatalf("Expected nil, but got %+v", j.Value)
	}
	if *j.Timestamp != 42 {
		t.Fatalf("Expected 42, but got %+v", *j.Timestamp)
	}

	// Upsert.
	i2 := &Object{}
	i2.UserID = 2
	i2.ID = "bar"
	i2.Value = []byte("greetings")

	i.Value = []byte("hello again")
	// Non-pointer should fail.
	if err := db.Upsert(*i, *i2); err == nil {
		t.Fatalf("Expected error, but found success")
	}
	// Pointer should succeed.
	if err := db.Upsert(i, i2); err != nil {
		t.Fatal(err)
	}

	if err := db.Get(j, 1, "bar"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(i, j) {
		t.Fatalf("Expected %+v, but got %+v", i, j)
	}

	// Replace.
	i.Value = []byte("goodbye again")
	i2.Value = []byte("fairwell")
	// Non-pointer should fail.
	if err := db.Replace(*i, *i2); err == nil {
		t.Fatalf("Expected error, but found success")
	}
	// Pointer should succeed.
	if err := db.Replace(i, i2); err != nil {
		t.Fatal(err)
	}

	if err := db.Get(j, 1, "bar"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(i, j) {
		t.Fatalf("Expected %+v, but got %+v", i, j)
	}

	{
		// Select into a slice of structs.
		var results []Object
		q := items.Select("*").Where(items.C("user_id").Eq(1))
		if err := db.Select(&results, q); err != nil {
			t.Fatal(err)
		} else if len(results) != 1 {
			t.Fatalf("Expected 1 result, but got %d", len(results))
		} else if !reflect.DeepEqual(*i, results[0]) {
			t.Fatalf("Expected %+v, but got %+v", *i, results[0])
		}
	}

	{
		// Select into a slice of pointers to structs.
		var results []*Object
		q := items.Select("*").Where(items.C("user_id").Eq(2))
		if err := db.Select(&results, q); err != nil {
			t.Fatal(err)
		} else if len(results) != 1 {
			t.Fatalf("Expected 1 result, but got %d", len(results))
		} else if !reflect.DeepEqual(i2, results[0]) {
			t.Fatalf("Expected %+v, but got %+v", i2, results[0])
		}
	}

	{
		// QueryRow.
		q := items.Select("*").Where(items.C("user_id").Eq(1))
		if err := db.QueryRow(q).StructScan(j); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(i, j) {
			t.Fatalf("Expected %+v, but got %+v", i, j)
		}
	}

	{
		// QueryRow with no matches.
		q := items.Select("*").Where(items.C("user_id").Eq(3))
		if err := db.QueryRow(q).StructScan(j); err != sql.ErrNoRows {
			t.Fatal("Expected failure, but found success")
		}
	}

	// QueryRow with more than one match.
	if err := db.QueryRow(items.Select("*")).StructScan(j); err != nil {
		t.Fatalf("Expected success, but found %s", err)
	}

	{
		// Select into an unregistered struct.
		count := &struct {
			N int `db:"total"`
		}{}
		q := items.Select(items.C("user_id").Count().As("total"))
		if err := db.QueryRow(q).StructScan(count); err != nil {
			t.Fatal(err)
		} else if count.N != 2 {
			t.Fatalf("Expected count of %d, but got %d", 2, count.N)
		}
	}

	{
		// Select into an int.
		var count int
		q := items.Select(items.C("user_id").Count())
		if err := db.QueryRow(q).Scan(&count); err != nil {
			t.Fatal(err)
		} else if count != 2 {
			t.Fatalf("Expected count of %d, but got %d", 2, count)
		}
	}

	{
		// Select into non-struct slice.
		var userIDs []int64
		q := items.Select(items.C("user_id"))
		if err := db.Select(&userIDs, q); err != nil {
			t.Fatal(err)
		} else if len(userIDs) != 2 {
			t.Fatalf("Expected count of %d, but got %d", 2, len(userIDs))
		}
		expected := []int64{1, 2}
		if !reflect.DeepEqual(expected, userIDs) {
			t.Fatalf("Expected %+v, but %+v", expected, userIDs)
		}
	}

	{
		// Select into non-struct slice.
		var userIDs []*int64
		q := items.Select(items.C("user_id"))
		if err := db.Select(&userIDs, q); err != nil {
			t.Fatal(err)
		} else if len(userIDs) != 2 {
			t.Fatalf("Expected count of %d, but got %d", 2, len(userIDs))
		}
		vals := []int64{1, 2}
		expected := []*int64{&vals[0], &vals[1]}
		if !reflect.DeepEqual(expected, userIDs) {
			t.Fatalf("Expected %+v, but %+v", expected, userIDs)
		}
	}

	// Non-pointer should fail.
	if _, err := db.Delete(*i); err == nil {
		t.Fatalf("Expected error, but found success")
	}
	// Pointer should succeed.
	if n, err := db.Delete(i); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("Expected 1 delete, but got %d", n)
	}
	if err := db.Get(i, "foo", "bar"); err == nil {
		t.Fatalf("Expected error, but found success")
	}
}

type User struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

func TestDBAutoIncrement(t *testing.T) {
	db := makeTestDB(t, usersDDL)
	defer db.Close()

	if _, err := db.BindModel("users", User{}); err != nil {
		t.Fatal(err)
	}

	// Auto-increment ids start at 1.
	e := &User{ID: 0, Name: "one"}
	if err := db.Insert(e); err != nil {
		t.Fatal(err)
	}
	if e.ID != 1 {
		t.Fatalf("Expected ID of 1, but got %d", e.ID)
	}

	// An auto-increment column can be manually specified.
	e.ID = 3
	e.Name = "three"
	if err := db.Insert(e); err != nil {
		t.Fatal(err)
	}
	if e.ID != 3 {
		t.Fatalf("Expected ID of 3, but got %d", e.ID)
	}

	// Insert multiple entries at once.
	e4 := &User{Name: "four"}
	e5 := &User{Name: "five"}
	e6 := &User{Name: "size"}
	if err := db.Insert(e4, e5, e6); err != nil {
		t.Fatal(err)
	}
	for i, e := range []*User{e4, e5, e6} {
		if e.ID != uint64(i+4) {
			t.Fatalf("Expected ID of %d, but got %d", i+4, e.ID)
		}
	}

	if n, err := db.Delete(e4, e5, e6); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("Expected 3, but got %d", n)
	}

	// Insert multiple entries with auto-increment values at once.
	e7 := &User{ID: 7, Name: "seven"}
	e8 := &User{ID: 8, Name: "eight"}
	e9 := &User{ID: 9, Name: "nine"}
	if err := db.Insert(e7, e8, e9); err != nil {
		t.Fatal(err)
	}
	for i, e := range []*User{e7, e8, e9} {
		if e.ID != uint64(i+7) {
			t.Fatalf("Expected ID of %d, but got %d", i+7, e.ID)
		}
	}

	// Insert multiple entries with mismatching auto-increment value presence.
	e10 := &User{ID: 10, Name: "seven"}
	e11 := &User{Name: "eight"}
	if err := db.Insert(e10, e11); err == nil {
		t.Fatalf("Expected error, but found success")
	}
}

func TestDBBatch(t *testing.T) {
	db := makeTestDB(t, objectsDDL, usersDDL)
	defer db.Close()

	if _, err := db.BindModel("objects", Object{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BindModel("users", User{}); err != nil {
		t.Fatal(err)
	}

	i := &Object{UserID: 1, ID: "bar", Value: "insert"}
	e := &User{ID: 0, Name: "insert"}
	if err := db.Insert(i, e); err != nil {
		t.Fatal(err)
	}

	checkObjects := func() {
		i2 := &Object{}
		if err := db.Get(i2, 1, "bar"); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(i, i2) {
			t.Fatalf("Expected %+v, but got %+v", i, i2)
		}
		e2 := &User{}
		if err := db.Get(e2, e.ID); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(e, e2) {
			t.Fatalf("Expected %+v, but got %+v", e, e2)
		}
	}
	checkObjects()

	i.Value = "replace"
	e.Name = "replace"
	if err := db.Replace(i, e); err != nil {
		t.Fatal(err)
	}
	checkObjects()

	i.Value = "update"
	e.Name = "update"
	if count, err := db.Update(i, e); err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("Expected count of %d, but got %d", 2, count)
	}
	checkObjects()

	i.Value = "upsert"
	e.Name = "upsert"
	if err := db.Upsert(i, e); err != nil {
		t.Fatal(err)
	}
	checkObjects()

	if count, err := db.Delete(i, e); err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("Expected count of %d, but got %d", 2, count)
	}
	if err := db.Get(i, "foo", "bar"); err == nil {
		t.Fatalf("Expected error, but found success")
	}
	if err := db.Get(e, e.ID); err == nil {
		t.Fatalf("Expected error, but found success")
	}
}

type HookedObject struct {
	UserID      int64  `db:"user_id"`
	ID          string `db:"object_id"`
	Timestamp   int64  `db:"timestamp"`
	Value       string `db:"value"`
	preDelete   func(Executor) error
	postDelete  func(Executor) error
	postGet     func(Executor) error
	preInsert   func(Executor) error
	postInsert  func(Executor) error
	preReplace  func(Executor) error
	postReplace func(Executor) error
	preUpdate   func(Executor) error
	postUpdate  func(Executor) error
	preUpsert   func(Executor) error
	postUpsert  func(Executor) error
}

func (h *HookedObject) PreDelete(exec Executor) error {
	return h.preDelete(exec)
}
func (h *HookedObject) PostDelete(exec Executor) error {
	return h.postDelete(exec)
}
func (h *HookedObject) PostGet(exec Executor) error {
	return h.postGet(exec)
}
func (h *HookedObject) PreInsert(exec Executor) error {
	return h.preInsert(exec)
}
func (h *HookedObject) PostInsert(exec Executor) error {
	return h.postInsert(exec)
}
func (h *HookedObject) PreReplace(exec Executor) error {
	return h.preReplace(exec)
}
func (h *HookedObject) PostReplace(exec Executor) error {
	return h.postReplace(exec)
}
func (h *HookedObject) PreUpdate(exec Executor) error {
	return h.preUpdate(exec)
}
func (h *HookedObject) PostUpdate(exec Executor) error {
	return h.postUpdate(exec)
}
func (h *HookedObject) PreUpsert(exec Executor) error {
	return h.preUpsert(exec)
}
func (h *HookedObject) PostUpsert(exec Executor) error {
	return h.postUpsert(exec)
}

func TestDBHooks(t *testing.T) {
	db := makeTestDB(t, objectsDDL)
	defer db.Close()

	if _, err := db.BindModel("objects", HookedObject{}); err != nil {
		t.Fatal(err)
	}

	counts := map[string]int{}

	makeCountingHook := func(name string) func(Executor) error {
		return func(Executor) error {
			counts[name]++
			return nil
		}
	}

	checkAndClearCounts := func(expected map[string]int) {
		if !reflect.DeepEqual(expected, counts) {
			t.Errorf("Expected %+v, but got %+v", expected, counts)
		}
		for k := range counts {
			delete(counts, k)
		}
	}

	i := &HookedObject{
		UserID:      1,
		ID:          "bar",
		Value:       "hello world",
		preDelete:   makeCountingHook("preDelete"),
		postDelete:  makeCountingHook("postDelete"),
		postGet:     makeCountingHook("postGet"),
		preInsert:   makeCountingHook("preInsert"),
		postInsert:  makeCountingHook("postInsert"),
		preReplace:  makeCountingHook("preReplace"),
		postReplace: makeCountingHook("postReplace"),
		preUpdate:   makeCountingHook("preUpdate"),
		postUpdate:  makeCountingHook("postUpdate"),
		preUpsert:   makeCountingHook("preUpsert"),
		postUpsert:  makeCountingHook("postUpsert"),
	}

	if err := db.Insert(i); err != nil {
		t.Fatal(err)
	}
	checkAndClearCounts(map[string]int{
		"preInsert":  1,
		"postInsert": 1,
	})

	if err := db.Replace(i); err != nil {
		t.Fatal(err)
	}
	checkAndClearCounts(map[string]int{
		"preReplace":  1,
		"postReplace": 1,
	})

	if _, err := db.Update(i); err != nil {
		t.Fatal(err)
	}
	checkAndClearCounts(map[string]int{
		"preUpdate":  1,
		"postUpdate": 1,
	})

	if err := db.Upsert(i); err != nil {
		t.Fatal(err)
	}
	checkAndClearCounts(map[string]int{
		"preUpsert":  1,
		"postUpsert": 1,
	})

	if err := db.Get(i, 1, "bar"); err != nil {
		t.Fatal(err)
	}
	checkAndClearCounts(map[string]int{
		"postGet": 1,
	})

	if _, err := db.Delete(i); err != nil {
		t.Fatal(err)
	}
	checkAndClearCounts(map[string]int{
		"preDelete":  1,
		"postDelete": 1,
	})
}

func TestDBAllowStringQueries(t *testing.T) {
	db := makeTestDB(t, objectsDDL)
	defer db.Close()
	db.AllowStringQueries = false

	items, err := db.BindModel("objects", Object{})
	if err != nil {
		t.Fatal(err)
	}

	i := &Object{UserID: 1, ID: "bar", Value: "hello world"}
	if err := db.Insert(i); err != nil {
		t.Fatal(err)
	}

	// Programmatic queries should succeed.
	var results []Object
	if err := db.Select(&results, items.Select("*")); err != nil {
		t.Fatalf("Expected success, but got %s", err)
	}

	// String queries should fail.
	if err := db.Select(&results, "SELECT * FROM item"); err == nil {
		t.Fatalf("Expected error, but found success")
	}
}

func TestBindModel_FailUnknownColumns(t *testing.T) {
	db := makeTestDB(t, objectsDDLWithUnmappedColumns)
	db.IgnoreUnmappedCols = false
	defer db.Close()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("Expected to recover an error")
		}
		str := "field 'unmapped' has no mapping"
		if !strings.Contains(r.(error).Error(), str) {
			t.Fatalf("Expected error to contain \"%s\", but got \"%s\"", str, r)
		}
	}()

	db.BindModel("objects", Object{})
	t.Fatal("Expected panic. Should not reach here")
}

func TestBindModel_FailNoPrimaryKey(t *testing.T) {
	db := makeTestDB(t, objectsDDLWithoutPrimaryKey)
	defer db.Close()

	_, err := db.BindModel("objects", Object{})
	str := "objects: table has no primary key"
	if err == nil {
		t.Fatalf("Expected missing primary key error, but got nil")
	} else if err.Error() != str {
		t.Fatalf("Unexpected error `%s`, expected `%s`", err, str)
	}
}

func TestSelect_FailOnUnknownColumns(t *testing.T) {
	db := makeTestDB(t, objectsDDL)
	db.IgnoreUnmappedCols = false
	defer db.Close()

	items, err := db.BindModel("objects", Object{})
	if err != nil {
		t.Fatal(err)
	}

	i := &Object{UserID: 1, ID: "bar", Value: "hello world"}
	if err := db.Insert(i); err != nil {
		t.Fatal(err)
	}

	db.Exec(objectsDDLAddUnmappedColumns)

	var results []Object
	err = db.Select(&results, items.Select("*"))
	if err == nil {
		t.Fatalf("Expected error, but got %+v", results)
	}
	str := "unable to find mapping for column 'unmapped'"
	if !strings.Contains(err.Error(), str) {
		t.Fatalf("Expected error to contain \"%s\", but got \"%s\"", str, err)
	}
}

func TestStructScan_FailOnUnknownColumns(t *testing.T) {
	db := makeTestDB(t, objectsDDL)
	db.IgnoreUnmappedCols = false
	defer db.Close()

	items, err := db.BindModel("objects", Object{})
	if err != nil {
		t.Fatal(err)
	}

	i := &Object{UserID: 1, ID: "bar", Value: "hello world"}
	if err := db.Insert(i); err != nil {
		t.Fatal(err)
	}

	db.Exec(objectsDDLAddUnmappedColumns)

	j := &Object{}
	q := items.Select("*").Where(items.C("user_id").Eq(1))
	err = db.QueryRow(q).StructScan(j)
	if err == nil {
		t.Fatal("Expected error, but got %+v", j)
	}
	str := "unable to find mapping for column 'unmapped'"
	if !strings.Contains(err.Error(), str) {
		t.Fatalf("Expected error to contain \"%s\", but got \"%s\"", str, err)
	}
}

type Object struct {
	UserID    int64  `db:"user_id"`
	ID        string `db:"object_id"`
	Timestamp int64  `db:"timestamp"`
	Value     string `db:"value"`
}

func makeItems(n int) []interface{} {
	var items []interface{}
	for i := 0; i < n; i++ {
		d := &Object{
			UserID: 1,
			ID:     fmt.Sprintf("%08d", i),
			Value:  fmt.Sprintf("hello %d", i),
		}
		items = append(items, d)
	}
	return items
}

func mustInsertItems(b *testing.B, db benchDB, items []interface{}) {
	const batchSize = 100000
	for i := 0; i < len(items); i += batchSize {
		e := i + batchSize
		if e >= len(items) {
			e = len(items)
		}
		if err := db.Insert(items[i:e]...); err != nil {
			b.Fatal(err)
		}
	}
}

type benchTx interface {
	Commit() error
	Delete(list ...interface{}) (int64, error)
	Get(obj interface{}, keys ...interface{}) error
	Insert(list ...interface{}) error
	Replace(list ...interface{}) error
	Update(list ...interface{}) (int64, error)
	Upsert(list ...interface{}) error
}

type benchDB interface {
	Begin() (benchTx, error)
	Close() error
	Insert(list ...interface{}) error
}

func benchmarkGet(b *testing.B, batchSize int, db benchDB) {
	defer db.Close()

	items := makeItems(b.N)
	mustInsertItems(b, db, items)

	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for _, obj := range items {
		i := obj.(*Object)
		if err := tx.Get(i, i.UserID, i.ID); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func benchmarkDelete(b *testing.B, batchSize int, db benchDB) {
	defer db.Close()

	items := makeItems(b.N)
	mustInsertItems(b, db, items)

	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < len(items); i += batchSize {
		e := i + batchSize
		if e >= len(items) {
			e = len(items)
		}
		if _, err := tx.Delete(items[i:e]...); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func benchmarkInsert(b *testing.B, batchSize int, db benchDB) {
	defer db.Close()

	items := makeItems(b.N)

	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < len(items); i += batchSize {
		e := i + batchSize
		if e >= len(items) {
			e = len(items)
		}
		if err := tx.Insert(items[i:e]...); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func benchmarkReplace(b *testing.B, batchSize int, db benchDB) {
	defer db.Close()

	items := makeItems(b.N)

	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < len(items); i += batchSize {
		e := i + batchSize
		if e >= len(items) {
			e = len(items)
		}
		if err := tx.Replace(items[i:e]...); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func benchmarkUpdate(b *testing.B, batchSize int, db benchDB) {
	defer db.Close()

	items := makeItems(b.N)
	mustInsertItems(b, db, items)

	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < len(items); i += batchSize {
		e := i + batchSize
		if e >= len(items) {
			e = len(items)
		}
		if _, err := tx.Update(items[i:e]...); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func benchmarkUpsert(b *testing.B, batchSize int, db benchDB) {
	defer db.Close()

	items := makeItems(b.N)

	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < len(items); i += batchSize {
		e := i + batchSize
		if e >= len(items) {
			e = len(items)
		}
		if err := tx.Upsert(items[i:e]...); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

type gorpBenchDB struct {
	*gorp.DbMap
}

func (db *gorpBenchDB) Begin() (benchTx, error) {
	tx, err := db.DbMap.Begin()
	if err != nil {
		return nil, err
	}
	return &gorpBenchTx{tx}, nil
}

func (db *gorpBenchDB) Close() error {
	return db.Db.Close()
}

type gorpBenchTx struct {
	*gorp.Transaction
}

func (tx *gorpBenchTx) Get(obj interface{}, keys ...interface{}) error {
	_, err := tx.Transaction.Get(obj, keys...)
	return err
}

func (tx *gorpBenchTx) Replace(list ...interface{}) error {
	return fmt.Errorf("unimplemented")
}

func (tx *gorpBenchTx) Upsert(list ...interface{}) error {
	return fmt.Errorf("unimplemented")
}

func makeGorpBenchDB(b *testing.B) benchDB {
	db := makeTestDB(b, objectsDDL)
	gorpDB := &gorp.DbMap{
		Db: db.DB,
		Dialect: gorp.MySQLDialect{
			Engine:   "InnoDB",
			Encoding: "UTF8",
		},
	}
	gorpDB.AddTableWithName(Object{}, "objects").SetKeys(false, "user_id", "object_id")
	return &gorpBenchDB{gorpDB}
}

func BenchmarkGorpGet(b *testing.B) {
	benchmarkGet(b, b.N, makeGorpBenchDB(b))
}

func BenchmarkGorpDelete(b *testing.B) {
	benchmarkDelete(b, 100000, makeGorpBenchDB(b))
}

func BenchmarkGorpInsert(b *testing.B) {
	benchmarkInsert(b, 100000, makeGorpBenchDB(b))
}

func BenchmarkGorpUpdate(b *testing.B) {
	benchmarkUpdate(b, 100000, makeGorpBenchDB(b))
}

type squalorBenchDB struct {
	*DB
}

func (db *squalorBenchDB) Begin() (benchTx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func makeSqlutilBenchDB(b *testing.B) benchDB {
	db := makeTestDB(b, objectsDDL)
	if _, err := db.BindModel("objects", Object{}); err != nil {
		b.Fatal(err)
	}
	return &squalorBenchDB{db}
}

func BenchmarkDBGet1(b *testing.B) {
	benchmarkGet(b, 1, makeSqlutilBenchDB(b))
}

func BenchmarkDBGetN(b *testing.B) {
	db := makeSqlutilBenchDB(b)

	items := makeItems(b.N)
	mustInsertItems(b, db, items)

	model, err := db.(*squalorBenchDB).GetModel(Object{})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]RawVal, len(items))
	vals := make(ValExprs, len(items))
	for i, obj := range items {
		ids[i].Val = obj.(*Object).ID
		vals[i] = &ids[i]
	}
	q := model.Select("*").
		Where(model.C("user_id").Eq(1).And(model.C("object_id").InTuple(ValTuple{vals})))
	var results []Object
	if err := tx.(*Tx).Select(&results, q); err != nil {
		b.Fatal(err)
	} else if len(results) != len(items) {
		b.Fatalf("Expected %d results, but got %d", len(items), len(results))
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDBDelete1(b *testing.B) {
	benchmarkDelete(b, 1, makeSqlutilBenchDB(b))
}

func BenchmarkDBDeleteN(b *testing.B) {
	benchmarkDelete(b, 100000, makeSqlutilBenchDB(b))
}

func BenchmarkDBInsert1(b *testing.B) {
	benchmarkInsert(b, 1, makeSqlutilBenchDB(b))
}

func BenchmarkDBInsertN(b *testing.B) {
	benchmarkInsert(b, 100000, makeSqlutilBenchDB(b))
}

func BenchmarkDBReplace1(b *testing.B) {
	benchmarkReplace(b, 1, makeSqlutilBenchDB(b))
}

func BenchmarkDBReplaceN(b *testing.B) {
	benchmarkReplace(b, 100000, makeSqlutilBenchDB(b))
}

func BenchmarkDBUpdate1(b *testing.B) {
	benchmarkUpdate(b, 1, makeSqlutilBenchDB(b))
}

func BenchmarkDBUpdateN(b *testing.B) {
	benchmarkUpdate(b, 100000, makeSqlutilBenchDB(b))
}

func BenchmarkDBUpsert1(b *testing.B) {
	benchmarkUpsert(b, 1, makeSqlutilBenchDB(b))
}

func BenchmarkDBUpsertN(b *testing.B) {
	benchmarkUpsert(b, 100000, makeSqlutilBenchDB(b))
}
