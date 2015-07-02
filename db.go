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
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"sync"
	"time"
)

// ErrMixedAutoIncrIDs is returned when attempting to insert multiple
// records with a mixture of set and unset auto increment ids. This
// case is difficult to handle correctly, so for now either we update
// all the ids, or none at all.
var ErrMixedAutoIncrIDs = errors.New("sql: auto increment column must be all set or unset")

// Executor defines the common interface for executing operations on a
// DB or on a Tx.
type Executor interface {
	Delete(list ...interface{}) (int64, error)
	Exec(query interface{}, args ...interface{}) (sql.Result, error)
	Get(dest interface{}, keys ...interface{}) error
	Insert(list ...interface{}) error
	Query(query interface{}, args ...interface{}) (*Rows, error)
	QueryRow(query interface{}, args ...interface{}) *Row
	Replace(list ...interface{}) error
	Select(dest interface{}, query interface{}, args ...interface{}) error
	Update(list ...interface{}) (int64, error)
	Upsert(list ...interface{}) error
}

var _ Executor = &DB{}
var _ Executor = &Tx{}

func writeStrings(buf *bytes.Buffer, strs ...string) {
	for _, s := range strs {
		if _, err := buf.WriteString(s); err != nil {
			panic(err)
		}
	}
}

type deletePlan struct {
	deleteBuilder *DeleteBuilder
	keyColumns    []ValExprBuilder
	traversals    [][]int
	hooks         deleteHooks
}

func makeDeletePlan(m *Model) deletePlan {
	p := deletePlan{}
	p.deleteBuilder = m.Delete()
	p.keyColumns = make([]ValExprBuilder, len(m.PrimaryKey.Columns))
	columns := make([]string, len(m.PrimaryKey.Columns))
	for i, col := range m.PrimaryKey.Columns {
		p.keyColumns[i] = m.Table.C(col.Name)
		columns[i] = col.Name
	}
	p.traversals = m.fields.getTraversals(columns)
	return p
}

type getPlan struct {
	selectBuilder *SelectBuilder
	keyColumns    []ValExprBuilder
	traversals    [][]int
	hooks         getHooks
}

func makeGetPlan(m *Model) getPlan {
	p := getPlan{}
	p.selectBuilder = m.selectAll()
	p.traversals = m.fields.getTraversals(m.mappedColNames)

	p.keyColumns = make([]ValExprBuilder, len(m.PrimaryKey.Columns))
	for i, col := range m.PrimaryKey.Columns {
		p.keyColumns[i] = m.Table.C(col.Name)
	}
	return p
}

type insertPlan struct {
	insertBuilder  *InsertBuilder
	replaceBuilder *ReplaceBuilder
	traversals     [][]int
	autoIncr       []int
	autoIncrInt    bool
	hooks          hooks
}

func makeInsertPlan(m *Model, replace bool) insertPlan {
	p := insertPlan{}
	var columns []interface{}
	for _, col := range m.mappedColumns {
		columns = append(columns, m.Table.C(col.Name))
		if col.AutoIncr {
			f, ok := m.fields[col.Name]
			if !ok {
				panic(fmt.Errorf("%s: unable to find field %s", m.Name, col))
			}
			p.autoIncr = f.Index
			switch f.Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				p.autoIncrInt = true
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				p.autoIncrInt = false
			default:
				panic(fmt.Errorf("%s: expecting int or uint for auto-increment field %s but got %s", m.Name, col, f.Type.Kind()))
			}

		}
	}
	if replace {
		p.replaceBuilder = m.Replace(columns...)
		p.hooks = replaceHooks{}
	} else {
		p.insertBuilder = m.Insert(columns...)
		p.hooks = insertHooks{}
	}
	p.traversals = m.fields.getTraversals(m.mappedColNames)
	return p
}

func makeUpsertPlan(m *Model) insertPlan {
	p := makeInsertPlan(m, false)
	// We're not able to process auto-increment columns on upsert. Don't
	// even try.
	p.autoIncr = nil

	primaryKey := map[string]bool{}
	for _, col := range m.PrimaryKey.Columns {
		primaryKey[col.Name] = true
	}
	for _, col := range m.mappedColumns {
		if col.AutoIncr || primaryKey[col.Name] {
			continue
		}
		p.insertBuilder.OnDupKeyUpdateColumn(col.Name)
	}

	p.hooks = upsertHooks{}
	return p
}

type updatePlan struct {
	updateBuilder   *UpdateBuilder
	setColumns      []ValExprBuilder
	setTraversals   [][]int
	whereColumns    []ValExprBuilder
	whereTraversals [][]int
	hooks           updateHooks
}

func makeUpdatePlan(m *Model) updatePlan {
	p := updatePlan{}
	p.updateBuilder = m.Update()

	primaryKey := map[string]bool{}
	whereColNames := make([]string, len(m.PrimaryKey.Columns))
	p.whereColumns = make([]ValExprBuilder, len(m.PrimaryKey.Columns))
	for i, col := range m.PrimaryKey.Columns {
		primaryKey[col.Name] = true
		p.whereColumns[i] = m.Table.C(col.Name)
		whereColNames[i] = col.Name
	}
	p.whereTraversals = m.fields.getTraversals(whereColNames)

	var setColumns []string
	for _, col := range m.mappedColumns {
		if col.AutoIncr || primaryKey[col.Name] {
			continue
		}
		setColumns = append(setColumns, col.Name)
		p.setColumns = append(p.setColumns, m.Table.C(col.Name))
	}
	p.setTraversals = m.fields.getTraversals(setColumns)
	return p
}

// A Model contains the precomputed data for a model binding to a
// table.
type Model struct {
	// The table the model is associated with.
	Table
	// The DB the model is associated with.
	db *DB
	// The mapping from column name to model object field info.
	fields fieldMap
	// All DB columns that are mapped in the model.
	mappedColumns  []*Column
	mappedColNames []string
	// The precomputed query plans.
	delete  deletePlan
	get     getPlan
	insert  insertPlan
	replace insertPlan
	update  updatePlan
	upsert  insertPlan
}

func newModel(db *DB, t reflect.Type, table Table) (*Model, error) {
	m := &Model{
		db:     db,
		Table:  table,
		fields: getDBFields(t),
	}
	m.mappedColumns = m.fields.getMappedColumns(m.Columns, db.IgnoreUnmappedCols)
	m.mappedColNames = getColumnNames(m.mappedColumns)
	m.delete = makeDeletePlan(m)
	m.get = makeGetPlan(m)
	m.insert = makeInsertPlan(m, false)
	m.replace = makeInsertPlan(m, true)
	m.update = makeUpdatePlan(m)
	m.upsert = makeUpsertPlan(m)
	return m, nil
}

func (m *Model) selectAll() *SelectBuilder {
	return m.Select(m.C(m.mappedColNames...))
}

func getColumnNames(columns []*Column) []string {
	var colNames []string
	for _, c := range columns {
		colNames = append(colNames, c.Name)
	}
	return colNames
}

func getInsert(m *Model) insertPlan {
	return m.insert
}

func getReplace(m *Model) insertPlan {
	return m.replace
}

func getUpsert(m *Model) insertPlan {
	return m.upsert
}

// stringSerializer is a wrapper around a string that implements Serializer.
type stringSerializer string

func (ss stringSerializer) Serialize(w Writer) error {
	_, err := io.WriteString(w, string(ss))
	return err
}

// DB is a wrapper around a sql.DB which also implements the
// squalor.Executor interface. DB is safe for concurrent use by
// multiple goroutines.
type DB struct {
	*sql.DB
	AllowStringQueries bool
	// Whether to ignore unmapped columns for the various DB function calls such as StructScan,
	// Select, Insert, BindModel, etc. When set to true, it can suppress column mapping validation
	// errors at DB migration time when new columns are added but the previous version of the binary
	// is still in use, either actively running or getting started up.
	//
	// The default is true that ignores the unmapped columns.
	// NOTE: Unmapped columns in primary keys are still not allowed.
	IgnoreUnmappedCols bool
	Logger             QueryLogger
	mu                 sync.RWMutex
	models             map[reflect.Type]*Model
	mappings           map[reflect.Type]fieldMap
}

// NewDB creates a new DB from an sql.DB.
func NewDB(db *sql.DB) *DB {
	return &DB{
		DB:                 db,
		AllowStringQueries: true,
		IgnoreUnmappedCols: true,
		Logger:             nil,
		models:             map[reflect.Type]*Model{},
		mappings:           map[reflect.Type]fieldMap{},
	}
}

func (db *DB) logQuery(query Serializer, exec Executor, start time.Time, err error) {
	if db.Logger == nil {
		return
	}

	executionTime := time.Now().Sub(start)
	db.Logger.Log(query, exec, executionTime, err)
}

// GetModel retrieves the model for the specified object. Obj must be
// a struct. An error is returned if obj has not been bound to a table
// via a call to BindModel.
func (db *DB) GetModel(obj interface{}) (*Model, error) {
	return db.getModel(reflect.TypeOf(obj))
}

func (db *DB) getModel(t reflect.Type) (*Model, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if model, ok := db.models[t]; ok {
		return model, nil
	}
	return nil, fmt.Errorf("unable to find model for '%s'", t)
}

func (db *DB) getMapping(t reflect.Type) fieldMap {
	db.mu.RLock()
	mapping := db.mappings[t]
	db.mu.RUnlock()

	if mapping != nil {
		return mapping
	}

	// Note that concurrent calls to getMapping for the same type might
	// create multiple (identical) mappings, only one of which will be
	// cached. This is fine as the mappings are readonly and nothing
	// using them relies on their identity.
	mapping = getDBFields(t)

	db.mu.Lock()
	db.mappings[t] = mapping
	db.mu.Unlock()
	return mapping
}

func (db *DB) getSerializer(query interface{}) (Serializer, error) {
	if t, ok := query.(Serializer); ok {
		return t, nil
	}

	if db.AllowStringQueries {
		switch t := query.(type) {
		case string:
			return stringSerializer(t), nil
		}
	}

	return nil, fmt.Errorf("unsupported query type %T", query)
}

// BindModel binds the supplied interface with the named table. You
// must bind the model for any object you wish to perform operations
// on. It is an error to bind the same model type more than once and a
// single model type can only be bound to a single table.
func (db *DB) BindModel(name string, obj interface{}) (*Model, error) {
	t := deref(reflect.TypeOf(obj))

	db.mu.Lock()
	m := db.models[t]
	db.mu.Unlock()

	if m != nil {
		return nil, fmt.Errorf("%s: model '%T' already defined", name, obj)
	}

	table, err := LoadTable(db.DB, name)
	if err != nil {
		return nil, err
	}
	if table.PrimaryKey == nil {
		return nil, fmt.Errorf("%s: table has no primary key", name)
	}

	m, err = newModel(db, t, *table)
	if err != nil {
		return nil, err
	}

	db.mu.Lock()
	db.models[t] = m
	db.mappings[t] = m.fields
	db.mu.Unlock()

	return m, nil
}

// MustBindModel binds the supplied interface with the named table,
// panicking if an error occurs.
func (db *DB) MustBindModel(name string, obj interface{}) *Model {
	model, err := db.BindModel(name, obj)
	if err != nil {
		panic(fmt.Errorf("%s: unable to bind model: %s", name, err))
	}
	return model
}

// Delete runs a batched SQL DELETE statement, grouping the objects by
// the model type of the list elements. List elements must be pointers
// to structs.
//
// On success, returns the number of rows deleted.
//
// Returns an error if an element in the list has not been registered
// with BindModel.
//
// Due to MySQL limitations, batch deletions are more restricted than
// insertions. The most natural implementation would be something
// like:
//
//   DELETE FROM <table> WHERE (<cols>...) IN ((<vals1>), (<vals2>), ...)
//
// This works except that it is spectactularly slow if there is more
// than one column in the primary key. MySQL changes this into a
// full table scan and then compares the primary key for each row
// with the "IN" set of values.
//
// Instead, we batch up deletions based on the first n-1 primary key
// columns. For a two column primary key this looks like:
//
//   DELETE FROM <table> WHERE <cols1>=<val1> and <col2> IN (<val2>...)
//
// If you're deleting a batch of objects where the first primary key
// column differs for each object this degrades to non-batched
// deletion. But if your first primary key column is identical then
// batching can work perfectly.
func (db *DB) Delete(list ...interface{}) (int64, error) {
	return deleteObjects(db, db, list)
}

// Exec executes a query without returning any rows. The args are for any
// placeholder parameters in the query.
func (db *DB) Exec(query interface{}, args ...interface{}) (sql.Result, error) {
	serializer, err := db.getSerializer(query)
	if err != nil {
		return nil, err
	}
	querystr, err := Serialize(serializer)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	argsConverted := argsConvert(args)
	result, err := db.DB.Exec(querystr, argsConverted...)
	db.logQuery(serializer, db, start, err)

	return result, err
}

// Get runs a SQL SELECT to fetch a single row. Keys must be the
// primary keys defined for the table. The order must match the order
// of the columns in the primary key.
//
// Returns an error if the object type has not been registered with
// BindModel.
func (db *DB) Get(dest interface{}, keys ...interface{}) error {
	return getObject(db, db, dest, keys)
}

// Insert runs a batched SQL INSERT statement, grouping the objects by
// the model type of the list elements. List elements must be pointers
// to structs.
//
// An object bound to a table with an auto-increment column will have
// its corresponding field filled in with the generated value if a
// pointer to the object was passed in "list".
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (db *DB) Insert(list ...interface{}) error {
	return insertObjects(db, db, getInsert, list)
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query. This is a
// small wrapper around sql.DB.Query that returns a *squalor.Rows
// instead.
func (db *DB) Query(query interface{}, args ...interface{}) (*Rows, error) {
	serializer, err := db.getSerializer(query)
	if err != nil {
		return nil, err
	}
	querystr, err := Serialize(serializer)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	argsConverted := argsConvert(args)
	rows, err := db.DB.Query(querystr, argsConverted...)
	db.logQuery(serializer, db, start, err)

	if err != nil {
		return nil, err
	}
	return &Rows{Rows: rows, db: db}, nil
}

// QueryRow executes a query that is expected to return at most one
// row. QueryRow always return a non-nil value. Errors are deferred
// until Row's Scan method is called. This is a small wrapper around
// sql.DB.QueryRow that returns a *squalor.Row instead.
func (db *DB) QueryRow(query interface{}, args ...interface{}) *Row {
	serializer, err := db.getSerializer(query)
	if err != nil {
		return &Row{rows: Rows{Rows: nil, db: nil}, err: err}
	}
	querystr, err := Serialize(serializer)
	if err != nil {
		return &Row{rows: Rows{Rows: nil, db: nil}, err: err}
	}

	start := time.Now()
	argsConverted := argsConvert(args)
	rows, err := db.DB.Query(querystr, argsConverted...)
	db.logQuery(serializer, db, start, err)

	return &Row{rows: Rows{Rows: rows, db: db}, err: err}
}

// Replace runs a batched SQL REPLACE statement, grouping the objects
// by the model type of the list elements. List elements must be
// pointers to structs.
//
// Note that REPLACE is effectively an INSERT followed by a DELETE and
// INSERT if the object already exists. The REPLACE may fail if the
// DELETE would violate foreign key constraints. Due to the batched
// nature of the Replace implementation it is not possible to
// accurately return the assignment of auto-increment values. Updating
// of an existing object will cause the auto-increment column to
// change.
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (db *DB) Replace(list ...interface{}) error {
	return insertObjects(db, db, getReplace, list)
}

// Select runs an arbitrary SQL query, unmarshalling the matching rows
// into the fields on the struct specified by dest. Args are the
// parameters to the SQL query.
//
// It is ok for dest to refer to a struct that has not been bound to a
// table. This allows querying for values that return transient
// columnts. For example, "SELECT count(*) ..." will return a "count"
// column.
//
// dest must be a pointer to a slice. Either *[]struct{} or
// *[]*struct{} is allowed.  It is mildly more efficient to use
// *[]struct{} due to the reduced use of reflection and allocation.
func (db *DB) Select(dest interface{}, q interface{}, args ...interface{}) error {
	return selectObjects(db, dest, q, args)
}

// Update runs a SQL UPDATE statement for each element in list. List
// elements may be structs or pointers to structs.
//
// On success, returns the number of rows updated.
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (db *DB) Update(list ...interface{}) (int64, error) {
	return updateObjects(db, db, list)
}

// Upsert runs a SQL INSERT ON DUPLICATE KEY UPDATE statement for each
// element in list. List elements must be pointers to structs.
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (db *DB) Upsert(list ...interface{}) error {
	return insertObjects(db, db, getUpsert, list)
}

// Begin begins a transaction and returns a *squalor.Tx instead of a
// *sql.Tx.
func (db *DB) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, DB: db}, nil
}

// Tx is a wrapper around sql.Tx which also implements the
// squalor.Executor interface.
type Tx struct {
	*sql.Tx
	DB        *DB
	preHooks  []PreCommit
	postHooks []PostCommit
}

// AddPreCommitHook adds a pre-commit hook to this transaction.
func (tx *Tx) AddPreCommitHook(pre PreCommit) {
	tx.preHooks = append(tx.preHooks, pre)
}

// AddPostCommitHook adds a post-commit hook to this transaction.
func (tx *Tx) AddPostCommitHook(post PostCommit) {
	tx.postHooks = append(tx.postHooks, post)
}

// Commit is a wrapper around sql.Tx.Commit() which also provides pre- and post-
// commit hooks.
func (tx *Tx) Commit() error {
	for _, pre := range tx.preHooks {
		if err := pre(tx); err != nil {
			return err
		}
	}
	err := tx.Tx.Commit()
	for _, post := range tx.postHooks {
		post(err)
	}
	return err
}

// Exec executes a query that doesn't return rows. For example: an
// INSERT and UPDATE.
func (tx *Tx) Exec(query interface{}, args ...interface{}) (sql.Result, error) {
	serializer, err := tx.DB.getSerializer(query)
	if err != nil {
		return nil, err
	}
	querystr, err := Serialize(serializer)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	argsConverted := argsConvert(args)
	result, err := tx.Tx.Exec(querystr, argsConverted...)
	tx.DB.logQuery(serializer, tx, start, err)

	return result, err
}

// Delete runs a batched SQL DELETE statement, grouping the objects by
// the model type of the list elements. List elements must be pointers
// to structs.
//
// On success, returns the number of rows deleted.
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (tx *Tx) Delete(list ...interface{}) (int64, error) {
	return deleteObjects(tx.DB, tx, list)
}

// Get runs a SQL SELECT to fetch a single row. Keys must be the
// primary keys defined for the table. The order must match the order
// of the columns in the primary key.
//
// Returns an error if the object type has not been registered with
// BindModel.
func (tx *Tx) Get(dest interface{}, keys ...interface{}) error {
	return getObject(tx.DB, tx, dest, keys)
}

// Insert runs a batched SQL INSERT statement, grouping the objects by
// the model type of the list elements. List elements must be pointers
// to structs.
//
// An object bound to a table with an auto-increment column will have
// its corresponding field filled in with the generated value if a
// pointer to the object was passed in "list".
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (tx *Tx) Insert(list ...interface{}) error {
	return insertObjects(tx.DB, tx, getInsert, list)
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query. This is a
// small wrapper around sql.Tx.Query that returns a *squalor.Rows
// instead.
func (tx *Tx) Query(query interface{}, args ...interface{}) (*Rows, error) {
	serializer, err := tx.DB.getSerializer(query)
	if err != nil {
		return nil, err
	}
	querystr, err := Serialize(serializer)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	argsConverted := argsConvert(args)
	rows, err := tx.Tx.Query(querystr, argsConverted...)
	tx.DB.logQuery(serializer, tx, start, err)

	if err != nil {
		return nil, err
	}
	return &Rows{Rows: rows, db: tx.DB}, nil
}

// QueryRow executes a query that is expected to return at most one
// row. QueryRow always return a non-nil value. Errors are deferred
// until Row's Scan method is called. This is a small wrapper around
// sql.Tx.QueryRow that returns a *squalor.Row instead.
func (tx *Tx) QueryRow(query interface{}, args ...interface{}) *Row {
	serializer, err := tx.DB.getSerializer(query)
	if err != nil {
		return &Row{rows: Rows{Rows: nil, db: nil}, err: err}
	}
	querystr, err := Serialize(serializer)
	if err != nil {
		return &Row{rows: Rows{Rows: nil, db: nil}, err: err}
	}

	start := time.Now()
	argsConverted := argsConvert(args)
	rows, err := tx.Tx.Query(querystr, argsConverted...)
	tx.DB.logQuery(serializer, tx, start, err)

	return &Row{rows: Rows{Rows: rows, db: tx.DB}, err: err}
}

// Replace runs a batched SQL REPLACE statement, grouping the objects
// by the model type of the list elements. List elements must be
// pointers to structs.
//
// Note that REPLACE is effectively an INSERT followed by a DELETE and
// INSERT if the object already exists. The REPLACE may fail if the
// DELETE would violate foreign key constraints. Due to the batched
// nature of the Replace implementation it is not possible to
// accurately return the assignment of auto-increment values. Updating
// of an existing object will cause the auto-increment column to
// change.
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (tx *Tx) Replace(list ...interface{}) error {
	return insertObjects(tx.DB, tx, getReplace, list)
}

// Select runs an arbitrary SQL query, unmarshalling the matching rows
// into the fields on the struct specified by dest. Args are the
// parameters to the SQL query.
//
// It is ok for dest to refer to a struct that has not been bound to a
// table. This allows querying for values that return transient
// columnts. For example, "SELECT count(*) ..." will return a "count"
// column.
//
// dest must be a pointer to a slice. Either *[]struct{} or
// *[]*struct{} is allowed.  It is mildly more efficient to use
// *[]struct{} due to the reduced use of reflection and allocation.
func (tx *Tx) Select(dest interface{}, q interface{}, args ...interface{}) error {
	return selectObjects(tx, dest, q, args)
}

// Update runs a SQL UPDATE statement for each element in list. List
// elements may be structs or pointers to structs.
//
// On success, returns the number of rows updated.
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (tx *Tx) Update(list ...interface{}) (int64, error) {
	return updateObjects(tx.DB, tx, list)
}

// Upsert runs a SQL INSERT ON DUPLICATE KEY UPDATE statement for each
// element in list. List elements must be pointers to structs.
//
// Returns an error if an element in the list has not been registered
// with BindModel.
func (tx *Tx) Upsert(list ...interface{}) error {
	return insertObjects(tx.DB, tx, getUpsert, list)
}

// Rows is a wrapper around sql.Rows which adds a StructScan method.
type Rows struct {
	*sql.Rows
	db      *DB
	structT reflect.Type
	zero    reflect.Value
	value   reflect.Value
	dest    []interface{}
}

// StructScan copies the columns in the current row into the struct
// pointed at by dest.
func (r *Rows) StructScan(dest interface{}) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer: %T", dest)
	}
	v = v.Elem()

	t := v.Type()
	if r.structT != t {
		r.structT = t
		if err := r.initScan(t); err != nil {
			return err
		}
	}

	if err := r.scanValue(); err != nil {
		return err
	}
	v.Set(r.value)
	return nil
}

func (r *Rows) initScan(t reflect.Type) error {
	if t.Kind() != reflect.Struct {
		// We're not scanning into a struct. Construct the value we'll
		// scan into and set up the "dest" slice which will be used for
		// scanning.
		ptr := reflect.New(t)
		r.value = ptr.Elem()
		r.dest = []interface{}{ptr.Interface()}
	} else {
		m := r.db.getMapping(t)
		// Fetch the column names in the result.
		cols, err := r.Rows.Columns()
		if err != nil {
			return err
		}

		// Set up the "dest" slice which we'll scan into. Note that the
		// "dest" slice remains the same for every row we scan. That is,
		// we're scanning each row into the same object ("value").
		r.dest = make([]interface{}, len(cols))
		r.value = reflect.New(t).Elem()
		for i, col := range cols {
			field, ok := m[col]
			if !ok {
				if !r.db.IgnoreUnmappedCols {
					return fmt.Errorf("unable to find mapping for column '%s'", col)
				}
				r.dest[i] = new(sql.RawBytes)
				continue
			}
			r.dest[i] = r.value.FieldByIndex(field.Index).Addr().Interface()
		}
	}
	r.zero = reflect.Zero(t)
	return nil
}

func (r *Rows) scanValue() error {
	// Clear out our value object in preparation for the scan.
	r.value.Set(r.zero)
	return r.Rows.Scan(r.dest...)
}

// Row is a wrapper around sql.Row which adds a StructScan method.
type Row struct {
	rows Rows
	err  error
}

// Scan copies the columns from the matched row into the values
// pointed at by dest. If more than one row matches the query, Scan
// uses the first row and discards the rest. If no row matches the
// query, Scan returns ErrNoRows.
func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	// TODO(bradfitz): for now we need to defensively clone all
	// []byte that the driver returned (not permitting
	// *RawBytes in Rows.Scan), since we're about to close
	// the Rows in our defer, when we return from this function.
	// the contract with the driver.Next(...) interface is that it
	// can return slices into read-only temporary memory that's
	// only valid until the next Scan/Close.  But the TODO is that
	// for a lot of drivers, this copy will be unnecessary.  We
	// should provide an optional interface for drivers to
	// implement to say, "don't worry, the []bytes that I return
	// from Next will not be modified again." (for instance, if
	// they were obtained from the network anyway) But for now we
	// don't care.
	defer r.rows.Close()
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	return r.rows.Close()
}

// StructScan copies the columns from the matched row into the struct
// pointed at by dest. If more than one row matches the query, Scan
// uses the first row and discards the rest. If no row matches the
// query, Scan returns ErrNoRows.
func (r *Row) StructScan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}

	defer r.rows.Close()

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := r.rows.StructScan(dest); err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	return r.rows.Close()
}

// Columns returns the column names. Columns returns an error if the
// row is closed, or if there was a deferred error from processing the
// query.
func (r *Row) Columns() ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.rows.Columns()
}

func groupObjects(db *DB, list []interface{}) (map[*Model][]interface{}, error) {
	objs := make(map[*Model][]interface{}, len(list))
	for _, obj := range list {
		objT := reflect.TypeOf(obj)
		if objT.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("obj must be a pointer: %T", obj)
		}
		model, err := db.getModel(objT.Elem())
		if err != nil {
			return nil, err
		}
		objs[model] = append(objs[model], obj)
	}
	return objs, nil
}

// keyInfo contains the start and end indices into a byte buffer for a
// key.
type keyInfo struct {
	start, end int
}

type rowInfo struct {
	keys []keyInfo
	obj  interface{}
}

// rowGrouper groups a set of rows by the first n-1 keys in each row.
type rowGrouper struct {
	n int // the number of keys in a row
	// The shared buffer of encoded keys. The keyInfo indexes point into
	// here.
	buf  []byte
	rows []rowInfo // the rows, each row containing n keys
}

func (s *rowGrouper) Len() int {
	return len(s.rows)
}

// rowKey returns a byte slice for the keys [begin, end] in row i. The
// returned byte slice can be compared against other rows for the same
// range of keys within a row. Note that the returned byte slice is a
// concatenation of the encoded keys. This is acceptable for comparing
// if keys are identical but does not allow for accurate less than
// comparisons.
func (s *rowGrouper) rowKey(i, begin, end int) []byte {
	r := s.rows[i]
	return s.buf[r.keys[begin].start:r.keys[end].end]
}

// compare compares the prefix of keys [0,n] in rows i and j.
func (s *rowGrouper) compare(i, j, n int) int {
	ik := s.rowKey(i, 0, n)
	jk := s.rowKey(j, 0, n)
	return bytes.Compare(ik, jk)
}

func (s *rowGrouper) Less(i, j int) bool {
	return s.compare(i, j, s.n-1) < 0
}

func (s *rowGrouper) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

// TODO(pmattis): The various *Model functions could really be methods
// on either Model or *Plan.
func deleteModel(model *Model, exec Executor, list []interface{}) (int64, error) {
	// Note: you might be tempted to think that the DELETE statement
	// could have the form:
	//
	//   DELETE FROM <table> WHERE (<col1>=<val1> AND <col2>=<val2>) OR (<col1>=<val3> ...)
	//
	// This works well when the number of rows to delete is smallish,
	// but somewhere around 500 rows (or perhaps it is the size of the
	// WHERE expression), MySQL switches to an inefficient form of
	// processing this statement. The result is that batch deletions
	// would switchover from being faster than single deletions, to
	// being much slower.

	// This is unfortunately complex. How do we group by the n-1 prefix
	// of primary key columns? First off, we generate the encoded SQL
	// values for all of the primary keys. Mildly complicated because we
	// want to avoid excessive memory allocation here.

	// buf will contain all of the encoded SQL values. We keep pointers
	// into it for each of the rows and each of the keys within the
	// row. But because the underlying []byte in buf can change when we
	// append more values, we have to use integer indices instead of
	// byte slices.
	var buf bytes.Buffer
	n := len(model.delete.traversals)
	keybuf := make([]keyInfo, len(list)*n)
	rows := make([]rowInfo, len(list))
	hooks := model.delete.hooks

	for j, obj := range list {
		v := reflect.Indirect(reflect.ValueOf(obj))
		if err := hooks.pre(obj, exec); err != nil {
			return -1, err
		}
		keys := keybuf[j*n : (j+1)*n]
		for i, traversal := range model.delete.traversals {
			start := buf.Len()
			err := encodeSQLValue(&buf, v.FieldByIndex(traversal).Interface())
			if err != nil {
				return -1, err
			}
			keys[i] = keyInfo{start, buf.Len()}
		}
		rows[j].keys = keys
		rows[j].obj = obj
	}

	// We've encoded all the row keys. Now sort the rows. This will
	// allow us to easily find all of the rows that are identical for
	// the prefix of n-1 columns.
	grouper := &rowGrouper{
		n:    n,
		buf:  buf.Bytes(),
		rows: rows,
	}
	sort.Sort(grouper)

	// Buffers for the encoded vals and arguments that will be used for
	// the AND and IN expression. The buffers are the max size to
	// minimize reallocations, though it is possible we'll only use a
	// handful of values in the same batch.
	valbuf := make([]EncodedVal, len(rows)+n-1)
	argbuf := make(ValExprs, 0, len(rows))
	var inTuple ValTuple
	inTuple.Exprs = argbuf

	// Initialize the and-expr. The and-expr is reused across all
	// batches, but we change the values for each batch. The andVals are
	// the last n-1 elements of valBuf.
	andVals := valbuf[len(rows):]
	var andExpr BoolExprBuilder
	for j := 0; j < n-1; j++ {
		key := model.delete.keyColumns[j].Eq(&andVals[j])
		if j == 0 {
			andExpr = key
		} else {
			andExpr = andExpr.And(key)
		}
	}

	var count int64
	var start int
	for i := range rows {
		// Add the IN value for the current row.
		valbuf[i].Val = grouper.rowKey(i, n-1, n-1)
		inTuple.Exprs = append(inTuple.Exprs, &valbuf[i])

		// Flush the batch if this is the last row or if the and-vals (the
		// first n-1 columns) differ from the next row.
		if i == len(rows)-1 ||
			(n > 1 && grouper.compare(i, i+1, n-2) != 0) {
			b := *model.delete.deleteBuilder
			inExpr := model.delete.keyColumns[n-1].InTuple(inTuple)
			if andExpr.BoolExpr == nil {
				b.Where(inExpr)
			} else {
				// Set the and-expr values for the first n-1 columns. These
				// values are identical for the rows in the range [start,i].
				for j := 0; j < n-1; j++ {
					andVals[j].Val = grouper.rowKey(i, j, j)
				}
				b.Where(andExpr.And(inExpr))
			}

			res, err := exec.Exec(&b)
			if err != nil {
				return -1, err
			}

			nrows, err := res.RowsAffected()
			if err != nil {
				return -1, err
			}
			count += nrows

			// Run the hooks immediately for the objects that have been
			// deleted.
			for j := start; j <= i; j++ {
				if err := hooks.post(rows[j].obj, exec); err != nil {
					return -1, err
				}
			}

			// Reset for the next batch.
			start = i + 1
			inTuple.Exprs = argbuf
		}
	}

	return count, nil
}

func deleteObjects(db *DB, exec Executor, list []interface{}) (int64, error) {
	objs, err := groupObjects(db, list)
	if err != nil {
		return -1, err
	}

	var count int64
	for model, list := range objs {
		nrows, err := deleteModel(model, exec, list)
		if err != nil {
			return -1, err
		}
		count += nrows
	}

	return count, nil
}

func getObject(db *DB, exec Executor, obj interface{}, keys []interface{}) error {
	objT := reflect.TypeOf(obj)
	if objT.Kind() != reflect.Ptr {
		return fmt.Errorf("obj must be a pointer: %T", obj)
	}
	objT = objT.Elem()
	model, err := db.getModel(objT)
	if err != nil {
		return err
	}

	if len(keys) != len(model.get.keyColumns) {
		return fmt.Errorf("incorrect keys specified %d != %d",
			len(keys), len(model.get.keyColumns))
	}

	q := *model.get.selectBuilder
	var where BoolExprBuilder
	for i := range model.get.keyColumns {
		e := model.get.keyColumns[i].Eq(keys[i])
		if i == 0 {
			where = e
		} else {
			where = where.And(e)
		}
	}
	q.Where(where)

	v := reflect.Indirect(reflect.ValueOf(obj))
	dest := make([]interface{}, len(model.get.traversals))
	for i, traversal := range model.get.traversals {
		dest[i] = v.FieldByIndex(traversal).Addr().Interface()
	}

	if err := exec.QueryRow(&q).Scan(dest...); err != nil {
		return err
	}

	return model.get.hooks.post(obj, exec)
}

func insertModel(model *Model, exec Executor, getPlan func(m *Model) insertPlan,
	list []interface{}) error {
	// This is a little trickier than might be expected because we want
	// to minimize allocations. Doing so is somewhat straightforward
	// because we know the number of objects of various types we're
	// going to need.
	plan := getPlan(model)
	n := len(plan.traversals)
	rows := make(Values, len(list))
	tuples := make([]ValTuple, len(list))
	rawbuf := make([]RawVal, len(list)*n)
	argbuf := make(ValExprs, len(list)*n)
	hooks := plan.hooks

	nAutoIncr := 0
	for j, obj := range list {
		v := reflect.Indirect(reflect.ValueOf(obj))
		if err := hooks.pre(obj, exec); err != nil {
			return err
		}

		if plan.autoIncr != nil {
			f := v.FieldByIndex(plan.autoIncr)
			if (plan.autoIncrInt && f.Int() != 0) || (!plan.autoIncrInt && f.Uint() != 0) {
				nAutoIncr++
			}
		}

		args := argbuf[j*n : (j+1)*n]
		raw := rawbuf[j*n : (j+1)*n]
		for i, traversal := range plan.traversals {
			raw[i].Val = v.FieldByIndex(traversal).Interface()
			args[i] = &raw[i]
		}
		tuples[j].Exprs = args
		rows[j] = &tuples[j]
	}

	if nAutoIncr != 0 && nAutoIncr != len(list) {
		return ErrMixedAutoIncrIDs
	}

	var serializer Serializer
	if plan.replaceBuilder != nil {
		b := *plan.replaceBuilder
		b.AddRows(rows)
		serializer = &b
	} else {
		b := *plan.insertBuilder
		b.AddRows(rows)
		serializer = &b
	}

	res, err := exec.Exec(serializer)
	if err != nil {
		return err
	}

	if plan.autoIncr != nil && nAutoIncr == 0 {
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		for _, obj := range list {
			v := reflect.ValueOf(obj).Elem()
			f := v.FieldByIndex(plan.autoIncr)
			if plan.autoIncrInt {
				f.SetInt(id)
			} else {
				f.SetUint(uint64(id))
			}
			id++
		}
	}

	for _, obj := range list {
		if err := hooks.post(obj, exec); err != nil {
			return err
		}
	}
	return nil
}

func insertObjects(db *DB, exec Executor, getPlan func(m *Model) insertPlan, list []interface{}) error {
	objs, err := groupObjects(db, list)
	if err != nil {
		return err
	}
	for model, list := range objs {
		err := insertModel(model, exec, getPlan, list)
		if err != nil {
			return err
		}
	}
	return nil
}

func selectObjects(exec Executor, dest interface{}, query interface{}, args []interface{}) error {
	sliceValue := reflect.ValueOf(dest)
	if sliceValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer to a slice: %T", dest)
	}
	sliceValue = sliceValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to a slice: %T", dest)
	}

	modelT := sliceValue.Type().Elem()
	// Are we returning a slice of structs or pointers to structs?
	ptrResults := modelT.Kind() == reflect.Ptr
	if ptrResults {
		modelT = modelT.Elem()
	}

	rows, err := exec.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err := rows.initScan(modelT); err != nil {
		return err
	}

	for rows.Next() {
		err := rows.scanValue()
		if err != nil {
			return err
		}

		var result reflect.Value
		if ptrResults {
			// Create a new object and clone it from the one we scanned into.
			result = reflect.New(modelT)
			result.Elem().Set(rows.value)
		} else {
			// Since we're appending structs to the results, the value is
			// implicitly cloned.
			result = rows.value
		}

		sliceValue = reflect.Append(sliceValue, result)
	}

	reflect.ValueOf(dest).Elem().Set(sliceValue)
	return rows.Err()
}

func updateModel(model *Model, exec Executor, list []interface{}) (int64, error) {
	b := &UpdateBuilder{}
	raw := make([]RawVal, len(model.update.whereTraversals))
	hooks := model.update.hooks

	var count int64
	for _, obj := range list {
		v := reflect.Indirect(reflect.ValueOf(obj))
		if err := hooks.pre(obj, exec); err != nil {
			return -1, err
		}

		*b = *model.update.updateBuilder
		for i, col := range model.update.setColumns {
			b.Set(col, v.FieldByIndex(model.update.setTraversals[i]).Interface())
		}
		var where BoolExprBuilder
		for i, traversal := range model.update.whereTraversals {
			raw[i].Val = v.FieldByIndex(traversal).Interface()
			e := model.update.whereColumns[i].Eq(raw[i])
			if i == 0 {
				where = e
			} else {
				where = where.And(e)
			}
		}
		b.Where(where)

		res, err := exec.Exec(b)
		if err != nil {
			return -1, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}
		count += rows

		if err := hooks.post(obj, exec); err != nil {
			return -1, err
		}
	}
	return count, nil
}

func updateObjects(db *DB, exec Executor, list []interface{}) (int64, error) {
	objs, err := groupObjects(db, list)
	if err != nil {
		return -1, err
	}

	var count int64
	for model, list := range objs {
		nrows, err := updateModel(model, exec, list)
		if err != nil {
			return -1, err
		}
		count += nrows
	}
	return count, nil
}
