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
	"errors"
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"
)

// singleCol has a primary key composed of a single column. See
// newTestStatementsDB.
type singleCol struct {
	A int `db:"a"`
	B int `db:"b"`
}

// multiCol has a primary key composed of multiple columns. See
// newTestStatementsDB.
type multiCol struct {
	A int `db:"a"`
	B int `db:"b"`
	C int `db:"c"`
	D int `db:"d"`
}

type singleColWithUnmapped singleCol

// singleColOptlock has a primary key composed of a single column. See
// newTestStatementsDB. Additionally, the V column is used for optimistic
// locking.
type singleColOptlock struct {
	A int `db:"a"`
	B int `db:"b"`
	V int `db:"v,optlock"`
}

func newTestStatementsDB(t *testing.T) *DB {
	db, _ := NewDB(nil)

	data := []struct {
		name         string
		model        interface{}
		keys         int
		unmappedCols []*Column
	}{
		{
			"single",
			singleCol{},
			1,
			nil,
		},
		{
			"multi",
			multiCol{},
			3,
			nil,
		},
		{
			"single_with_unmapped",
			singleColWithUnmapped{},
			1,
			[]*Column{
				&Column{
					Name:     "unmapped",
					Nullable: true,
				},
			},
		},
		{
			"single_ol",
			singleColOptlock{},
			1,
			nil,
		},
	}
	for _, d := range data {
		table := NewTable(d.name, d.model)
		table.PrimaryKey = &Key{
			Name:    "PRIMARY",
			Primary: true,
			Unique:  true,
			Columns: table.Columns[:d.keys],
		}
		table.Columns = append(table.Columns, d.unmappedCols...)
		for _, col := range d.unmappedCols {
			table.ColumnMap[col.Name] = col
		}
		modelT := reflect.TypeOf(d.model)
		m, err := newModel(db, modelT, *table)
		if err != nil {
			t.Fatal(err)
		}
		db.models[modelT] = m
		db.mappings[modelT] = m.fields
	}
	return db
}

type dummyResult struct {
}

func (r dummyResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r dummyResult) RowsAffected() (int64, error) {
	return 1, nil
}

type recordingExecutor struct {
	*DB
	exec  []string
	query []string
}

func (r *recordingExecutor) Exec(stmt interface{}, args ...interface{}) (sql.Result, error) {
	return r.ExecContext(context.Background(), stmt, args...)
}

func (r *recordingExecutor) ExecContext(_ context.Context, stmt interface{}, args ...interface{}) (sql.Result, error) {
	var querystr string
	var err error

	switch t := stmt.(type) {
	case string:
		querystr = t
		return r.Exec(t, args...)
	case Serializer:
		querystr, err = Serialize(t)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unexpected stmt type")
	}

	if len(args) != 0 {
		panic(fmt.Errorf("expected 0 args: %+v", args))
	}

	r.exec = append(r.exec, querystr)
	return dummyResult{}, nil
}

func (r *recordingExecutor) QueryRow(query interface{}, args ...interface{}) *Row {
	return r.QueryRowContext(context.Background(), query, args...)
}

func (r *recordingExecutor) QueryRowContext(_ context.Context, query interface{}, args ...interface{}) *Row {
	if len(args) != 0 {
		panic(fmt.Errorf("expected 0 args: %+v", args))
	}

	if s, ok := query.(string); ok {
		r.query = append(r.query, s)
	} else if serializer, ok := query.(Serializer); ok {
		s, err := Serialize(serializer)
		if err != nil {
			return &Row{err: err}
		}
		r.query = append(r.query, s)
	}

	return &Row{err: errors.New("ignored")}
}

func TestDBDeleteStatements(t *testing.T) {
	// Test that the constructed DELETE statements match our
	// expectations.
	db := newTestStatementsDB(t)

	testCases := []struct {
		list     []interface{}
		expected []string
	}{
		{[]interface{}{&singleCol{1, 1}},
			[]string{
				"DELETE FROM `single` WHERE `single`.`a` IN (1)",
			},
		},
		{[]interface{}{&singleCol{1, 1}, &singleCol{2, 2}, &singleCol{3, 3}},
			[]string{
				"DELETE FROM `single` WHERE `single`.`a` IN (1, 2, 3)",
			},
		},
		{[]interface{}{&multiCol{1, 2, 3, 4}},
			[]string{
				"DELETE FROM `multi` WHERE (`multi`.`a` = 1 AND `multi`.`b` = 2 AND `multi`.`c` IN (3))",
			},
		},
		{[]interface{}{&multiCol{1, 2, 3, 4}, &multiCol{1, 2, 4, 5}},
			[]string{
				"DELETE FROM `multi` WHERE (`multi`.`a` = 1 AND `multi`.`b` = 2 AND `multi`.`c` IN (3, 4))",
			},
		},
		{[]interface{}{&multiCol{1, 2, 3, 4}, &multiCol{1, 3, 4, 5}},
			[]string{
				"DELETE FROM `multi` WHERE (`multi`.`a` = 1 AND `multi`.`b` = 2 AND `multi`.`c` IN (3))",
				"DELETE FROM `multi` WHERE (`multi`.`a` = 1 AND `multi`.`b` = 3 AND `multi`.`c` IN (4))",
			},
		},
	}

	for _, c := range testCases {
		recorder := &recordingExecutor{DB: db}
		model, err := db.getModel(deref(reflect.TypeOf(c.list[0])))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := deleteModel(context.Background(), model, recorder, c.list); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(c.expected, recorder.exec) {
			t.Errorf("Expected %+v, but got %+v", c.expected, recorder.exec)
		}
	}
}

func TestDBGetStatements(t *testing.T) {
	// Test that the constructed SELECT statements match our
	// expectations.
	db := newTestStatementsDB(t)

	testCases := []struct {
		obj      interface{}
		keys     []interface{}
		expected string
	}{
		{&singleCol{}, []interface{}{1},
			"SELECT `single`.`a`, `single`.`b` FROM `single` WHERE `single`.`a` = 1",
		},
		{&multiCol{}, []interface{}{1, 2, 3},
			"SELECT `multi`.`a`, `multi`.`b`, `multi`.`c`, `multi`.`d` " +
				"FROM `multi` WHERE (`multi`.`a` = 1 AND `multi`.`b` = 2 AND `multi`.`c` = 3)",
		},
		{&singleColWithUnmapped{}, []interface{}{1},
			"SELECT `single_with_unmapped`.`a`, `single_with_unmapped`.`b` " +
				"FROM `single_with_unmapped` WHERE `single_with_unmapped`.`a` = 1",
		},
	}

	for _, c := range testCases {
		recorder := &recordingExecutor{DB: db}
		if err := getObject(context.Background(), db, recorder, c.obj, c.keys); err == nil {
			t.Fatal("Expected ignored error, but found success")
		}
		if !reflect.DeepEqual([]string{c.expected}, recorder.query) {
			t.Errorf("Expected %+v, but got %+v", c.expected, recorder.query)
		}
	}
}

func TestDBSelectAllStatements(t *testing.T) {
	// Test that the constructed SELECT statements match our
	// expectations.
	db := newTestStatementsDB(t)

	testCases := []struct {
		obj      interface{}
		expected string
	}{
		{&singleCol{},
			"SELECT `single`.`a`, `single`.`b` FROM `single`",
		},
		{&multiCol{},
			"SELECT `multi`.`a`, `multi`.`b`, `multi`.`c`, `multi`.`d` FROM `multi`",
		},
		{&singleColWithUnmapped{},
			"SELECT `single_with_unmapped`.`a`, `single_with_unmapped`.`b`, `single_with_unmapped`.`unmapped` " +
				"FROM `single_with_unmapped`",
		},
	}

	for _, c := range testCases {
		recorder := &recordingExecutor{DB: db}
		model, err := db.getModel(deref(reflect.TypeOf(c.obj)))
		if err != nil {
			t.Errorf("Unable to find model for %v: %v", c.obj, err)
			continue
		}
		recorder.QueryRow(model.Select(model.All()))
		if !reflect.DeepEqual([]string{c.expected}, recorder.query) {
			t.Errorf("Expected %+v, but got %+v", c.expected, recorder.query)
		}
	}
}

func TestDBSelectAllMappedStatements(t *testing.T) {
	// Test that the constructed SELECT statements match our
	// expectations.
	db := newTestStatementsDB(t)

	testCases := []struct {
		obj      interface{}
		expected string
	}{
		{&singleCol{},
			"SELECT `single`.`a`, `single`.`b` FROM `single`",
		},
		{&multiCol{},
			"SELECT `multi`.`a`, `multi`.`b`, `multi`.`c`, `multi`.`d` FROM `multi`",
		},
		{&singleColWithUnmapped{},
			"SELECT `single_with_unmapped`.`a`, `single_with_unmapped`.`b` " +
				"FROM `single_with_unmapped`",
		},
	}

	for _, c := range testCases {
		recorder := &recordingExecutor{DB: db}
		model, err := db.getModel(deref(reflect.TypeOf(c.obj)))
		if err != nil {
			t.Errorf("Unable to find model for %v: %v", c.obj, err)
			continue
		}
		recorder.QueryRow(model.Select(model.AllMapped()))
		if !reflect.DeepEqual([]string{c.expected}, recorder.query) {
			t.Errorf("Expected %+v, but got %+v", c.expected, recorder.query)
		}
	}
}

func TestDBInsertStatements(t *testing.T) {
	// Test that the constructed INSERT/REPLACE statements match our
	// expectations.
	db := newTestStatementsDB(t)

	testCases := []struct {
		plan     func(m *Model) insertPlan
		list     []interface{}
		expected string
	}{
		{getInsert,
			[]interface{}{&singleCol{1, 1}},
			"INSERT INTO `single` (`a`, `b`) VALUES (1, 1)",
		},
		{getInsert,
			[]interface{}{&singleCol{1, 1}, &singleCol{2, 2}},
			"INSERT INTO `single` (`a`, `b`) VALUES (1, 1), (2, 2)",
		},
		{getInsert,
			[]interface{}{&multiCol{1, 2, 3, 4}},
			"INSERT INTO `multi` (`a`, `b`, `c`, `d`) VALUES (1, 2, 3, 4)",
		},
		{getInsert,
			[]interface{}{&multiCol{1, 2, 3, 4}, &multiCol{5, 6, 7, 8}},
			"INSERT INTO `multi` (`a`, `b`, `c`, `d`) VALUES (1, 2, 3, 4), (5, 6, 7, 8)",
		},
		{getReplace,
			[]interface{}{&singleCol{1, 1}},
			"REPLACE INTO `single` (`a`, `b`) VALUES (1, 1)",
		},
		{getReplace,
			[]interface{}{&singleCol{1, 1}, &singleCol{2, 2}},
			"REPLACE INTO `single` (`a`, `b`) VALUES (1, 1), (2, 2)",
		},
		{getReplace,
			[]interface{}{&multiCol{1, 2, 3, 4}},
			"REPLACE INTO `multi` (`a`, `b`, `c`, `d`) VALUES (1, 2, 3, 4)",
		},
		{getReplace,
			[]interface{}{&multiCol{1, 2, 3, 4}, &multiCol{5, 6, 7, 8}},
			"REPLACE INTO `multi` (`a`, `b`, `c`, `d`) VALUES (1, 2, 3, 4), (5, 6, 7, 8)",
		},
		{getUpsert,
			[]interface{}{&singleCol{1, 1}},
			"INSERT INTO `single` (`a`, `b`) VALUES (1, 1) " +
				"ON DUPLICATE KEY UPDATE `single`.`b` = VALUES(`single`.`b`)",
		},
		{getUpsert,
			[]interface{}{&singleCol{1, 1}, &singleCol{2, 2}},
			"INSERT INTO `single` (`a`, `b`) VALUES (1, 1), (2, 2) " +
				"ON DUPLICATE KEY UPDATE `single`.`b` = VALUES(`single`.`b`)",
		},
		{getUpsert,
			[]interface{}{&multiCol{1, 2, 3, 4}},
			"INSERT INTO `multi` (`a`, `b`, `c`, `d`) VALUES (1, 2, 3, 4) " +
				"ON DUPLICATE KEY UPDATE `multi`.`d` = VALUES(`multi`.`d`)",
		},
		{getUpsert,
			[]interface{}{&multiCol{1, 2, 3, 4}, &multiCol{5, 6, 7, 8}},
			"INSERT INTO `multi` (`a`, `b`, `c`, `d`) VALUES (1, 2, 3, 4), (5, 6, 7, 8) " +
				"ON DUPLICATE KEY UPDATE `multi`.`d` = VALUES(`multi`.`d`)",
		},
	}

	for _, c := range testCases {
		recorder := &recordingExecutor{DB: db}
		model, err := db.getModel(deref(reflect.TypeOf(c.list[0])))
		if err != nil {
			t.Fatal(err)
		}
		if err := insertModel(context.Background(), model, recorder, c.plan, c.list); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual([]string{c.expected}, recorder.exec) {
			t.Errorf("Expected %+v, but got %+v", c.expected, recorder.exec)
		}
	}
}

func TestDBUpdateStatements(t *testing.T) {
	// Test that the constructed UPDATE statements match our
	// expectations.
	db := newTestStatementsDB(t)

	testCases := []struct {
		list     []interface{}
		expected []string
	}{
		{[]interface{}{&singleCol{1, 2}},
			[]string{
				"UPDATE `single` SET `single`.`b` = 2 WHERE `single`.`a` = 1",
			},
		},
		{[]interface{}{&singleCol{1, 2}, &singleCol{3, 4}},
			[]string{
				"UPDATE `single` SET `single`.`b` = 2 WHERE `single`.`a` = 1",
				"UPDATE `single` SET `single`.`b` = 4 WHERE `single`.`a` = 3",
			},
		},
		{[]interface{}{&multiCol{1, 2, 3, 4}},
			[]string{
				"UPDATE `multi` SET `multi`.`d` = 4 " +
					"WHERE (`multi`.`a` = 1 AND `multi`.`b` = 2 AND `multi`.`c` = 3)",
			},
		},
		{[]interface{}{&multiCol{1, 2, 3, 4}, &multiCol{5, 6, 7, 8}},
			[]string{
				"UPDATE `multi` SET `multi`.`d` = 4 " +
					"WHERE (`multi`.`a` = 1 AND `multi`.`b` = 2 AND `multi`.`c` = 3)",
				"UPDATE `multi` SET `multi`.`d` = 8 " +
					"WHERE (`multi`.`a` = 5 AND `multi`.`b` = 6 AND `multi`.`c` = 7)",
			},
		},
		{[]interface{}{&singleColOptlock{1, 2, 3}},
			[]string{
				"UPDATE `single_ol` SET `single_ol`.`b` = 2, `single_ol`.`v` = `single_ol`.`v`+1 " +
					"WHERE (`single_ol`.`a` = 1 AND `single_ol`.`v` = 3)",
			},
		},
	}

	for _, c := range testCases {
		recorder := &recordingExecutor{DB: db}
		model, err := db.getModel(deref(reflect.TypeOf(c.list[0])))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := updateModel(context.Background(), model, recorder, c.list); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(c.expected, recorder.exec) {
			t.Errorf("Expected %+v, but got %+v", c.expected, recorder.exec)
		}
	}
}
