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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"text/tabwriter"
)

// Column models a database column.
type Column struct {
	Name     string
	AutoIncr bool
	Nullable bool
	sqlType  string
}

func (c *Column) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s\t%s", c.Name, c.sqlType)
	if c.Nullable {
		fmt.Fprintf(&buf, "\tNULL")
	} else {
		fmt.Fprintf(&buf, "\tNOT NULL")
	}
	if c.AutoIncr {
		fmt.Fprintf(&buf, " AUTO INCREMENT")
	}
	return buf.String()
}

type byName []*Column

func (n byName) Len() int {
	return len(n)
}

func (n byName) Less(i, j int) bool {
	return n[i].Name < n[j].Name
}

func (n byName) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

type keyColumn struct {
	seq  int
	name string
}

type bySeq []keyColumn

func (s bySeq) Len() int {
	return len(s)
}

func (s bySeq) Less(i, j int) bool {
	return s[i].seq < s[j].seq
}

func (s bySeq) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Key models a key (index) for a database table. A key is composed of
// one or more columns.
type Key struct {
	Name    string
	Primary bool
	Unique  bool
	Columns []*Column
}

func (k *Key) String() string {
	var buf bytes.Buffer
	if k.Primary {
		fmt.Fprintf(&buf, "PRIMARY ")
	} else if k.Unique {
		fmt.Fprintf(&buf, "UNIQUE ")
	}
	fmt.Fprintf(&buf, "KEY")
	if !k.Primary {
		fmt.Fprintf(&buf, " %s", k.Name)
	}
	fmt.Fprintf(&buf, "\t(")
	var sep string
	for _, col := range k.Columns {
		fmt.Fprintf(&buf, "%s%s", sep, col.Name)
		sep = ","
	}
	fmt.Fprintf(&buf, ")")
	return buf.String()
}

func (k *Key) matches(cols ...string) bool {
	if len(k.Columns) != len(cols) {
		return false
	}
	for i := range cols {
		if k.Columns[i].Name != cols[i] {
			return false
		}
	}
	return true
}

// Table models a database table containing column and key definitions.
type Table struct {
	Name       string
	Alias      string
	Columns    []*Column
	ColumnMap  map[string]*Column
	PrimaryKey *Key
	Keys       []*Key
	KeyMap     map[string]*Key
}

func makeTable(name string) *Table {
	return &Table{
		Name:      name,
		ColumnMap: make(map[string]*Column),
		KeyMap:    make(map[string]*Key),
	}
}

// NewTable constructs a table from a model struct. The resulting
// table is less detailed than one created from LoadTable due to the
// lack of keys of type info.
func NewTable(name string, model interface{}) *Table {
	t := makeTable(name)
	m := getDBFields(reflect.TypeOf(model))
	for k := range m {
		col := &Column{Name: k}
		t.Columns = append(t.Columns, col)
		t.ColumnMap[k] = col
	}
	sort.Sort(byName(t.Columns))
	return t
}

// NewAliasedTable constructs a table with an alias from a model struct. The
// alias will be used in column names and in joins. The resulting
// table is less detailed than one created from LoadTable due to the
// lack of keys of type info.
func NewAliasedTable(name, alias string, model interface{}) *Table {
	t := NewTable(name, model)
	t.Alias = alias
	return t
}

// LoadTable loads a table's definition from a database.
func LoadTable(db *sql.DB, name string) (*Table, error) {
	t := makeTable(name)
	err := t.loadColumns(db)
	if err != nil {
		return nil, err
	}
	err = t.loadKeys(db)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Table) String() string {
	var buf bytes.Buffer
	tab := tabwriter.NewWriter(&buf, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tab, "%s:\n", t.getName())
	fmt.Fprintf(tab, "  columns:\n")
	for _, col := range t.Columns {
		fmt.Fprintf(tab, "    %s\n", col)
	}
	fmt.Fprintf(tab, "  keys:\n")
	for _, key := range t.Keys {
		fmt.Fprintf(tab, "    %s\n", key)
	}
	tab.Flush()
	return buf.String()
}

// GetKey retrieves a key from the table definition.
func (t *Table) GetKey(cols ...string) *Key {
	for _, key := range t.Keys {
		if key.matches(cols...) {
			return key
		}
	}
	return nil
}

// C returns an expression for the specified list of columns. An error
// expression is created if any of the columns do not exist in the table. An
// error expression is created if no columns are specified.
func (t *Table) C(cols ...string) ValExprBuilder {
	if len(cols) == 0 {
		return ValExprBuilder{makeErrVal("no columns specified")}
	}

	if len(cols) == 1 {
		name := cols[0]
		if _, ok := t.ColumnMap[name]; !ok {
			return ValExprBuilder{makeErrVal("unknown column: %s", name)}
		}
		return ValExprBuilder{&ColName{Name: name, Qualifier: t.getName()}}
	}
	list := make([]interface{}, len(cols))
	for i, name := range cols {
		if _, ok := t.ColumnMap[name]; !ok {
			list[i] = makeErrVal("unknown column: %s", name)
		} else {
			list[i] = &ColName{Name: name, Qualifier: t.getName()}
		}
	}
	return ValExprBuilder{makeValTuple(list)}
}

// All returns an expression for all of the columns in the table in
// the order in which they are defined in the table (the order of
// Table.Columns). Note that returned expression is a tuple of
// columns, not a star expression.
func (t *Table) All() ValExprBuilder {
	list := make([]interface{}, len(t.Columns))
	for i, col := range t.Columns {
		list[i] = &ColName{Name: col.Name, Qualifier: t.getName()}
	}
	return ValExprBuilder{makeValTuple(list)}
}

// InnerJoin creates an INNER JOIN statement builder. Note that inner
// join and join are synonymous in MySQL. Inner join is used here for
// clarity.
func (t *Table) InnerJoin(other *Table) *JoinBuilder {
	return makeJoinBuilder("INNER JOIN", t, other)
}

// LeftJoin creates a LEFT JOIN statement builder.
func (t *Table) LeftJoin(other *Table) *JoinBuilder {
	return makeJoinBuilder("LEFT JOIN", t, other)
}

// RightJoin creates a RIGHT JOIN statement builder.
func (t *Table) RightJoin(other *Table) *JoinBuilder {
	return makeJoinBuilder("RIGHT JOIN", t, other)
}

// Delete creates a DELETE statement builder.
func (t *Table) Delete() *DeleteBuilder {
	return makeDeleteBuilder(t.getName())
}

// Insert creates an INSERT statement builder.
func (t *Table) Insert(cols ...interface{}) *InsertBuilder {
	return makeInsertBuilder(t, cols...)
}

// Replace creates a REPLACE statement builder.
func (t *Table) Replace(cols ...interface{}) *ReplaceBuilder {
	return makeReplaceBuilder(t, cols...)
}

// Select creates a SELECT statement builder.
func (t *Table) Select(exprs ...interface{}) *SelectBuilder {
	return makeSelectBuilder(t, exprs...)
}

// Update creates an UPDATE statement builder.
func (t *Table) Update() *UpdateBuilder {
	return makeUpdateBuilder(t)
}

func (t *Table) validateFields(m fieldMap) error {
	// Verify all of the model columns exist in the table.
	for name, field := range m {
		c1 := t.ColumnMap[name]
		if c1 == nil {
			return fmt.Errorf("%s: model column '%s' not found in table", t.getName(), name)
		}
		if err := validateModelType(c1.sqlType, field.Type); err != nil {
			return fmt.Errorf("%s: '%s': %s", t.getName(), name, err)
		}
	}
	// Verify all of the table columns exist in the model.
	for name := range t.ColumnMap {
		if _, ok := m[name]; !ok {
			return fmt.Errorf("%s: table column '%s' not found in model", t.getName(), name)
		}
	}
	return nil
}

// ValidateModel validates that the model is compatible for the
// table's schema. It checks that every column in the database is
// mapped to a field in the model and every field in the model has a
// corresponding column in the table schema.
func (t *Table) ValidateModel(model interface{}) error {
	m := getDBFields(reflect.TypeOf(model))
	return t.validateFields(m)
}

// getName will return table's alias if its not blank
// Otherwise it will use the table's name.
func (t *Table) getName() string {
	if t.Alias != "" {
		return t.Alias
	}
	return t.Name
}

// loadColumns loads a table's columns from a database. MySQL
// specific.
func (t *Table) loadColumns(db *sql.DB) error {
	rows, err := db.Query("SHOW FULL COLUMNS FROM " + t.getName())
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	var (
		col      string
		sqlType  string
		nullable string
		extra    string
	)
	m := map[string]interface{}{
		"Field": &col,
		"Type":  &sqlType,
		"Null":  &nullable,
		"Extra": &extra,
	}

	vals := make([]interface{}, len(cols))
	for i, n := range cols {
		if v, ok := m[n]; ok {
			vals[i] = v
		} else {
			vals[i] = &sql.RawBytes{}
		}
	}

	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			return err
		}
		c := &Column{
			Name:     col,
			Nullable: strings.EqualFold(nullable, "YES"),
			AutoIncr: strings.Contains(extra, "auto_increment"),
			sqlType:  sqlType,
		}
		t.Columns = append(t.Columns, c)
		t.ColumnMap[c.Name] = c
	}
	return nil
}

func (t *Table) tableExpr() TableExpr {
	ate := &AliasedTableExpr{
		Expr: &TableName{Name: t.Name},
	}
	if t.Alias != "" {
		ate.As = t.Alias
	}

	return ate
}

func (t *Table) tableExists(name string) bool {
	return t.getName() == name
}

func (t *Table) column(name string) *ColName {
	parts := strings.Split(name, ".")
	if len(parts) == 2 {
		if parts[0] != t.getName() {
			return nil
		}
		name = parts[1]
	}
	if _, ok := t.ColumnMap[name]; !ok {
		return nil
	}
	return &ColName{Name: name, Qualifier: t.getName()}
}

func (t *Table) columnCount(name string) int {
	if _, ok := t.ColumnMap[name]; !ok {
		return 0
	}
	return 1
}

// loadKeys loads a table's keys (indexes) from a database. MySQL
// specific.
func (t *Table) loadKeys(db *sql.DB) error {
	rows, err := db.Query("SHOW INDEX FROM " + t.getName())
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	var (
		nonUnique bool
		keyName   string
		seq       int
		colName   string
	)
	m := map[string]interface{}{
		"Non_unique":   &nonUnique,
		"Key_name":     &keyName,
		"Seq_in_index": &seq,
		"Column_name":  &colName,
	}

	vals := make([]interface{}, len(cols))
	for i, n := range cols {
		if v, ok := m[n]; ok {
			vals[i] = v
		} else {
			vals[i] = &sql.RawBytes{}
		}
	}

	keys := map[string][]keyColumn{}
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			return err
		}
		keys[keyName] = append(keys[keyName], keyColumn{
			seq:  seq,
			name: colName,
		})

		if _, ok := t.KeyMap[keyName]; !ok {
			k := &Key{
				Name:   keyName,
				Unique: !nonUnique,
			}
			t.Keys = append(t.Keys, k)
			t.KeyMap[keyName] = k
		}
	}

	for n, k := range keys {
		sort.Sort(bySeq(k))
		var cols []*Column
		for _, c := range k {
			cols = append(cols, t.ColumnMap[c.name])
		}
		t.KeyMap[n].Columns = cols
	}

	t.PrimaryKey = t.KeyMap["PRIMARY"]
	if t.PrimaryKey != nil {
		t.PrimaryKey.Primary = true
	}

	return nil
}
