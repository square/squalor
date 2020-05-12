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
	"regexp"
	"strings"
	"time"
)

// DeleteBuilder aids the construction of DELETE statements, providing
// methods for setting the clauses of a delete statement.
type DeleteBuilder struct {
	Delete
}

func makeDeleteBuilder(table abstractTable, tables ...*Table) *DeleteBuilder {
	var tableNames TableNames
	for _, table := range tables {
		tableNames = append(tableNames, &TableName{Name: table.Name})
	}
	b := &DeleteBuilder{}
	b.Delete.Table = table
	b.Delete.TableNames = tableNames
	return b
}

// Limit sets the LIMIT clause for the statement, replacing any
// existing limit clause.
func (b *DeleteBuilder) Limit(count interface{}) *DeleteBuilder {
	if b.Delete.Limit == nil {
		b.Delete.Limit = &Limit{}
	}
	b.Delete.Limit.Rowcount = makeValExpr(count)
	return b
}

// OrderBy sets the ORDER BY clause for the statement, replacing any
// existing order by clause.
func (b *DeleteBuilder) OrderBy(exprs ...interface{}) *DeleteBuilder {
	b.Delete.OrderBy = makeOrderBy(exprs...)
	return b
}

// Where sets the WHERE clause for the statement, replacing any
// existing where clause.
func (b *DeleteBuilder) Where(expr BoolExpr) *DeleteBuilder {
	b.Delete.Where = &Where{
		Type: astWhere,
		Expr: unwrapBoolExpr(expr),
	}
	return b
}

// Comments sets comments for the statement, replacing any
// existing comments.
func (b *DeleteBuilder) Comments(comments []string) *DeleteBuilder {
	b.Delete.Comments = comments
	return b
}

// abstractTable represents either a concrete table or the result of
// joining two tables.
type abstractTable interface {
	tableExpr() TableExpr
	tableExists(name string) bool
	column(name string) *ColName
	columnCount(name string) int
}

// The Selectable interface is a type for structs that provide the Select function.
// The types that implement this interface are Table and JoinBuilder.
// This interface is not used by Squalor itself, it is provided so that users can create functions
// that can accept either of these two types.
type Selectable interface {
	Select(exprs ...interface{}) *SelectBuilder
}

var _ Selectable = &JoinBuilder{}
var _ Selectable = &Table{}

// The Deletable interface is a type for structs that provide the Delete function.
// The types that implement this interface are Table and JoinBuilder.
// This interface is not used by Squalor itself, it is provided so that users can create functions
// that can accept either of these two types.
type Deletable interface {
	Delete(table ...*Table) *DeleteBuilder
}

var _ Deletable = &JoinBuilder{}
var _ Deletable = &Table{}

// JoinBuilder aids the construction of JOIN expressions, providing
// methods for specifying the join condition.
type JoinBuilder struct {
	JoinTableExpr
	leftTable  abstractTable
	rightTable abstractTable
}

func makeJoinBuilder(join string, left, right abstractTable) *JoinBuilder {
	j := &JoinBuilder{}
	j.Join = join
	j.leftTable = left
	j.LeftExpr = left.tableExpr()
	j.rightTable = right
	j.RightExpr = right.tableExpr()
	return j
}

// InnerJoin creates a INNER JOIN statement builder
func (b *JoinBuilder) InnerJoin(other *Table) *JoinBuilder {
	return makeJoinBuilder("INNER JOIN", b, other)
}

// LeftJoin creates a LEFT JOIN statement builder.
func (b *JoinBuilder) LeftJoin(other *Table) *JoinBuilder {
	return makeJoinBuilder("LEFT JOIN", b, other)
}

// RightJoin creates a RIGHT JOIN statement builder.
func (b *JoinBuilder) RightJoin(other *Table) *JoinBuilder {
	return makeJoinBuilder("RIGHT JOIN", b, other)
}

// Select creates a SELECT statement builder.
func (b *JoinBuilder) Select(exprs ...interface{}) *SelectBuilder {
	return makeSelectBuilder(b, exprs...)
}

// Delete creates a DELETE statement builder.
func (b *JoinBuilder) Delete(tables ...*Table) *DeleteBuilder {
	return makeDeleteBuilder(b, tables...)
}

// On sets an ON join condition for the expression, replacing any
// existing join condition.
func (b *JoinBuilder) On(expr BoolExpr) *JoinBuilder {
	b.Cond = &OnJoinCond{Expr: unwrapBoolExpr(expr)}
	return b
}

// Using sets a USING join condition for the expression, replacing any
// existing join condition. The columns must exist in both the left
// and right sides of the join expression.
func (b *JoinBuilder) Using(cols ...interface{}) *JoinBuilder {
	var vals Columns
	for _, c := range cols {
		var name string
		switch t := c.(type) {
		case string:
			name = t
		case ValExprBuilder:
			if n, ok := t.ValExpr.(*ColName); ok {
				name = n.Name
				break
			}
		}
		var v ValExpr
		if len(name) == 0 {
			v = makeErrVal("unsupported type %T: %v", c, c)
		} else if b.leftTable.column(name) == nil ||
			b.rightTable.column(name) == nil {
			v = makeErrVal("invalid join column: %s", name)
		} else {
			v = &ColName{Name: name}
		}
		vals = append(vals, &NonStarExpr{Expr: v})
	}
	b.Cond = &UsingJoinCond{Cols: vals}
	return b
}

func (b *JoinBuilder) tableExpr() TableExpr {
	return &b.JoinTableExpr
}

func (b *JoinBuilder) tableExists(name string) bool {
	return b.leftTable.tableExists(name) || b.rightTable.tableExists(name)
}

func (b *JoinBuilder) column(name string) *ColName {
	if col := b.leftTable.column(name); col != nil {
		return col
	}
	return b.rightTable.column(name)
}

func (b *JoinBuilder) columnCount(name string) int {
	return b.leftTable.columnCount(name) + b.rightTable.columnCount(name)
}

// InsertBuilder aids the construction of INSERT statements, providing
// methods for adding rows to the statement.
type InsertBuilder struct {
	Insert
	table *Table
}

func makeInsertBuilder(table *Table, kind string, cols ...interface{}) *InsertBuilder {
	b := &InsertBuilder{
		table: table,
	}
	b.Insert.Kind = kind
	b.Insert.Table = &TableName{
		Name: table.Name,
	}
	for _, c := range cols {
		var name string
		switch t := c.(type) {
		case string:
			name = t
		case ValExprBuilder:
			if n, ok := t.ValExpr.(*ColName); ok {
				name = n.Name
				break
			}
		}
		var v ValExpr
		if len(name) == 0 {
			v = makeErrVal("unsupported type %T: %v", c, c)
		} else if table.column(name) == nil {
			v = makeErrVal("invalid insert column: %s", name)
		} else {
			v = &ColName{Name: name}
		}
		b.Insert.Columns = append(b.Insert.Columns, &NonStarExpr{Expr: v})
	}
	return b
}

// Add appends a single row of values to the statement.
func (b *InsertBuilder) Add(vals ...interface{}) *InsertBuilder {
	var rows Values
	if b.Insert.Rows != nil {
		rows = b.Insert.Rows.(Values)
	}
	b.Insert.Rows = append(rows, makeValTuple(vals))
	return b
}

// AddRows appends multiple rows of values to the statement.
func (b *InsertBuilder) AddRows(rows Values) *InsertBuilder {
	if b.Insert.Rows == nil {
		b.Insert.Rows = rows
	} else {
		b.Insert.Rows = append(b.Insert.Rows.(Values), rows...)
	}
	return b
}

// Comments sets comments for the statement, replacing any
// existing comments.
func (b *InsertBuilder) Comments(comments []string) *InsertBuilder {
	b.Insert.Comments = comments
	return b
}

// OnDupKeyUpdate specifies an ON DUPLICATE KEY UPDATE expression to
// be performed when a duplicate primary key is encountered during
// insertion. The specified column must exist within the table being
// inserted into.
func (b *InsertBuilder) OnDupKeyUpdate(col interface{}, val interface{}) *InsertBuilder {
	b.Insert.OnDup = append(b.Insert.OnDup, makeUpdateExpr(b.table, col, val))
	return b
}

// OnDupKeyUpdateColumn specifies on ON DUPLICATE KEY UPDATE
// expression to be performed when a duplicate primary key is
// encountered during insertion. The specified column must exist
// within the table being inserted into. The value to use for updating
// is taken from the corresponding column value in the row being
// inserted.
func (b *InsertBuilder) OnDupKeyUpdateColumn(col interface{}) *InsertBuilder {
	colName := getColName(b.table, col)
	val := &FuncExpr{Name: "VALUES", Exprs: []SelectExpr{&NonStarExpr{Expr: colName}}}
	return b.OnDupKeyUpdate(col, val)
}

// ReplaceBuilder aids the construction of REPLACE expressions,
// providing methods for adding rows to the statement.
type ReplaceBuilder struct {
	Insert
	table *Table
}

func makeReplaceBuilder(table *Table, cols ...interface{}) *ReplaceBuilder {
	b := &ReplaceBuilder{
		table: table,
	}
	b.Insert.Kind = "REPLACE"
	b.Insert.Table = &TableName{
		Name: table.Name,
	}
	for _, c := range cols {
		var name string
		switch t := c.(type) {
		case string:
			name = t
		case ValExprBuilder:
			if n, ok := t.ValExpr.(*ColName); ok {
				name = n.Name
				break
			}
		}
		var v ValExpr
		if len(name) == 0 {
			v = makeErrVal("unsupported type %T: %v", c, c)
		} else if table.column(name) == nil {
			v = makeErrVal("invalid replace column: %s", name)
		} else {
			v = &ColName{Name: name}
		}
		b.Insert.Columns = append(b.Insert.Columns, &NonStarExpr{Expr: v})
	}
	return b
}

// Add appends a single row of values to the statement.
func (b *ReplaceBuilder) Add(vals ...interface{}) *ReplaceBuilder {
	var rows Values
	if b.Insert.Rows != nil {
		rows = b.Insert.Rows.(Values)
	}
	b.Insert.Rows = append(rows, makeValTuple(vals))
	return b
}

// AddRows appends multiple rows of values to the statement.
func (b *ReplaceBuilder) AddRows(rows Values) *ReplaceBuilder {
	if b.Insert.Rows == nil {
		b.Insert.Rows = rows
	} else {
		b.Insert.Rows = append(b.Insert.Rows.(Values), rows...)
	}
	return b
}

// Comments sets comments for the statement, replacing any
// existing comments.
func (b *ReplaceBuilder) Comments(comments []string) *ReplaceBuilder {
	b.Insert.Comments = comments
	return b
}

// SelectBuilder aids the construction of SELECT statements, providing
// methods for setting the clauses of the select statement.
type SelectBuilder struct {
	Select
	table abstractTable
}

func makeSelectBuilder(table abstractTable, exprs ...interface{}) *SelectBuilder {
	b := &SelectBuilder{table: table}
	for _, e := range exprs {
		b.addSelectExpr(e)
	}
	b.From = append(b.From, table.tableExpr())
	return b
}

func (b *SelectBuilder) addSelectExpr(expr interface{}) {
	var s SelectExpr
	switch v := expr.(type) {
	case StrVal:
		s = b.stringToSelectExpr(string(v))
	case string:
		s = b.stringToSelectExpr(v)
	case ValExprBuilder:
		switch t := v.ValExpr.(type) {
		case StrVal:
			s = b.stringToSelectExpr(string(t))
		case ValTuple:
			for _, q := range t.Exprs {
				b.addSelectExpr(q)
			}
			return
		default:
			s = &NonStarExpr{Expr: v.ValExpr}
		}
	case Expr:
		s = &NonStarExpr{Expr: v}
	case *NonStarExpr:
		s = v
	case SelectExpr:
		s = v
	default:
		s = &NonStarExpr{Expr: makeValExpr(expr)}
	}
	b.Exprs = append(b.Exprs, s)
}

func (b *SelectBuilder) stringToSelectExpr(v string) SelectExpr {
	if v == "*" {
		return &StarExpr{}
	}
	parts := strings.Split(v, ".")
	if len(parts) > 2 {
		return &NonStarExpr{
			Expr: makeErrVal("invalid select expression: %s", v),
		}
	}
	if len(parts) == 2 {
		if !b.table.tableExists(parts[0]) {
			return &NonStarExpr{
				Expr: makeErrVal("unknown table: %s", parts[0]),
			}
		}
		if parts[1] == "*" {
			return &StarExpr{TableName: parts[0]}
		}
	}
	if len(parts) == 1 && b.table.columnCount(v) > 1 {
		return &NonStarExpr{
			Expr: makeErrVal("ambiguous column: %s", v),
		}
	}
	// Note that column() will internally split v into table name and
	// column name.
	if col := b.table.column(v); col != nil {
		return &NonStarExpr{Expr: col}
	}
	return &NonStarExpr{
		Expr: makeErrVal("unknown column: %s", v),
	}
}

// Distinct sets the DISTINCT tag on the statement causing duplicate
// row results to be removed.
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.Select.Distinct = "DISTINCT "
	return b
}

// ForUpdate sets the FOR UPDATE tag on the statement causing the
// result rows to be locked (dependent on the specific MySQL storage
// engine).
func (b *SelectBuilder) ForUpdate() *SelectBuilder {
	b.Select.Lock = " FOR UPDATE"
	return b
}

// WithSharedLock sets the LOCK IN SHARE MODE tag on the statement
// causing the result rows to be read locked (dependent on the
// specific MySQL storage engine).
func (b *SelectBuilder) WithSharedLock() *SelectBuilder {
	b.Select.Lock = " LOCK IN SHARE MODE"
	return b
}

// Where sets the WHERE clause for the statement, replacing any
// existing where clause.
func (b *SelectBuilder) Where(expr BoolExpr) *SelectBuilder {
	b.Select.Where = &Where{
		Type: astWhere,
		Expr: unwrapBoolExpr(expr),
	}
	return b
}

// Comments sets comments for the statement, replacing any
// existing comments.
func (b *SelectBuilder) Comments(comments []string) *SelectBuilder {
	b.Select.Comments = comments
	return b
}

// Having sets the HAVING clause for the statement, replacing any
// existing having clause.
func (b *SelectBuilder) Having(expr BoolExpr) *SelectBuilder {
	b.Select.Having = &Where{
		Type: astHaving,
		Expr: unwrapBoolExpr(expr),
	}
	return b
}

// GroupBy sets the GROUP BY clause for the statement, replacing any
// existing group by clause.
func (b *SelectBuilder) GroupBy(vals ...ValExpr) *SelectBuilder {
	for i := range vals {
		vals[i] = unwrapValExpr(vals[i])
	}
	b.Select.GroupBy = GroupBy(vals)
	return b
}

// OrderBy sets the ORDER BY clause for the statement, replacing any
// existing order by clause.
func (b *SelectBuilder) OrderBy(exprs ...interface{}) *SelectBuilder {
	b.Select.OrderBy = makeOrderBy(exprs...)
	return b
}

// Limit sets the LIMIT clause for the statement, replacing any
// existing limit clause.
func (b *SelectBuilder) Limit(count interface{}) *SelectBuilder {
	if b.Select.Limit == nil {
		b.Select.Limit = &Limit{}
	}
	b.Select.Limit.Rowcount = makeValExpr(count)
	return b
}

// Offset sets the OFFSET clause for the statement. It is an error to
// set the offset before setting the limit.
func (b *SelectBuilder) Offset(offset interface{}) *SelectBuilder {
	if b.Select.Limit == nil {
		panic("offset without limit")
	}
	b.Select.Limit.Offset = makeValExpr(offset)
	return b
}

// UpdateBuilder aids the construction of UPDATE statements, providing
// methods for specifying which columns are to be updated and setting
// other clauses of the update statement.
type UpdateBuilder struct {
	Update
	table *Table
}

func makeUpdateBuilder(table *Table) *UpdateBuilder {
	b := &UpdateBuilder{table: table}
	b.Update.Table = &TableName{
		Name: table.Name,
	}
	return b
}

// Limit sets the limit clause for the statement, replacing any
// existing limit clause.
func (b *UpdateBuilder) Limit(count interface{}) *UpdateBuilder {
	if b.Update.Limit == nil {
		b.Update.Limit = &Limit{}
	}
	b.Update.Limit.Rowcount = makeValExpr(count)
	return b
}

// OrderBy sets the order by clause for the statement, replacing any
// existing order by clause.
func (b *UpdateBuilder) OrderBy(exprs ...interface{}) *UpdateBuilder {
	b.Update.OrderBy = makeOrderBy(exprs...)
	return b
}

// Set appends a Set expression to the statement. The specified column
// must exist within the table being updated.
func (b *UpdateBuilder) Set(col interface{}, val interface{}) *UpdateBuilder {
	b.Update.Exprs = append(b.Update.Exprs, makeUpdateExpr(b.table, col, val))
	return b
}

// Where sets the where clause for the statement, replacing any
// existing where clause.
func (b *UpdateBuilder) Where(expr BoolExpr) *UpdateBuilder {
	b.Update.Where = &Where{
		Type: astWhere,
		Expr: unwrapBoolExpr(expr),
	}
	return b
}

// Comments sets comments for the statement, replacing any
// existing comments.
func (b *UpdateBuilder) Comments(comments []string) *UpdateBuilder {
	b.Update.Comments = comments
	return b
}

// ValExprBuilder aids the construction of boolean expressions from
// values such as "foo == 1" or "bar IN ('a', 'b', 'c')" and value
// expressions such as "count + 1".
type ValExprBuilder struct {
	ValExpr
}

func makeValExpr(arg interface{}) ValExpr {
	switch t := arg.(type) {
	case ValExprBuilder:
		return t.ValExpr
	case ValExpr:
		return t
	}
	return RawVal{arg}
}

func unwrapValExpr(expr ValExpr) ValExpr {
	if b, ok := expr.(ValExprBuilder); ok {
		return b.ValExpr
	}
	return expr
}

func (b ValExprBuilder) makeComparisonExpr(op string, expr ValExpr) BoolExprBuilder {
	return BoolExprBuilder{
		&ComparisonExpr{
			Operator: op,
			Left:     b.ValExpr,
			Right:    expr,
		}}
}

// As creates an AS (alias) expression.
func (b ValExprBuilder) As(s string) SelectExpr {
	if !isValidIdentifier(s) {
		return &NonStarExpr{Expr: makeErrVal("invalid AS identifier: %s", s)}
	}
	return &NonStarExpr{Expr: b.ValExpr, As: s}
}

// Eq creates a = comparison expression.
func (b ValExprBuilder) Eq(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astEQ, makeValExpr(val))
}

// Neq creates a != comparison expression.
func (b ValExprBuilder) Neq(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astNE, makeValExpr(val))
}

// NullSafeEq creates a <=> comparison expression that is safe for use
// when the either the left or right value of the expression may be
// NULL. The null safe equal operator performs an equality comparison
// like the = operator, but returns 1 rather than NULL if both
// operands are NULL, and 0 rather than NULL if one operand is NULL.
func (b ValExprBuilder) NullSafeEq(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astNSE, makeValExpr(val))
}

// Lt creates a < comparison expression.
func (b ValExprBuilder) Lt(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astLT, makeValExpr(val))
}

// Lte creates a <= comparison expression.
func (b ValExprBuilder) Lte(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astLE, makeValExpr(val))
}

// Gt creates a > comparison expression.
func (b ValExprBuilder) Gt(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astGT, makeValExpr(val))
}

// Gte creates a >= comparison expression.
func (b ValExprBuilder) Gte(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astGE, makeValExpr(val))
}

// In creates an IN expression from a list of values.
func (b ValExprBuilder) In(list ...interface{}) BoolExprBuilder {
	return b.InTuple(makeValTuple(list))
}

// InTuple creates an IN expression from a tuple.
func (b ValExprBuilder) InTuple(tuple Tuple) BoolExprBuilder {
	if valTuple, ok := tuple.(ValTuple); ok {
		if len(valTuple.Exprs) == 0 {
			return b.makeComparisonExpr(astIn, makeErrVal("empty list"))
		}
	}
	return b.makeComparisonExpr(astIn, tuple)
}

// NotIn creates a NOT IN expression from a list of values.
func (b ValExprBuilder) NotIn(list ...interface{}) BoolExprBuilder {
	return b.NotInTuple(makeValTuple(list))
}

// NotInTuple creates a NOT IN expression from a tuple.
func (b ValExprBuilder) NotInTuple(tuple Tuple) BoolExprBuilder {
	if valTuple, ok := tuple.(ValTuple); ok {
		if len(valTuple.Exprs) == 0 {
			return b.makeComparisonExpr(astNotIn, makeErrVal("empty list"))
		}
	}
	return b.makeComparisonExpr(astNotIn, tuple)
}

// Like creates a LIKE expression.
func (b ValExprBuilder) Like(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astLike, makeValExpr(val))
}

// NotLike creates a NOT LIKE expression.
func (b ValExprBuilder) NotLike(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astNotLike, makeValExpr(val))
}

// RegExp creates a REGEXP expression.
func (b ValExprBuilder) RegExp(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astRegExp, makeValExpr(val))
}

// NotRegExp creates a NOT REGEXP expression.
func (b ValExprBuilder) NotRegExp(val interface{}) BoolExprBuilder {
	return b.makeComparisonExpr(astNotRegExp, makeValExpr(val))
}

func (b ValExprBuilder) makeRangeCond(
	op string, from interface{}, to interface{}) BoolExprBuilder {
	return BoolExprBuilder{
		&RangeCond{
			Operator: op,
			Left:     b.ValExpr,
			From:     makeValExpr(from),
			To:       makeValExpr(to),
		}}
}

// Between creates a BETWEEN expression.
func (b ValExprBuilder) Between(from interface{}, to interface{}) BoolExprBuilder {
	return b.makeRangeCond(astBetween, from, to)
}

// NotBetween creates a NOT BETWEEN expression.
func (b ValExprBuilder) NotBetween(from interface{}, to interface{}) BoolExprBuilder {
	return b.makeRangeCond(astNotBetween, from, to)
}

func (b ValExprBuilder) makeNullCheck(op string) BoolExprBuilder {
	return BoolExprBuilder{
		&NullCheck{
			Operator: op,
			Expr:     b.ValExpr,
		}}
}

// IsNull creates an IS NULL expression.
func (b ValExprBuilder) IsNull() BoolExprBuilder {
	return b.makeNullCheck(astIsNull)
}

// IsNotNull creates an IS NOT NULL expression.
func (b ValExprBuilder) IsNotNull() BoolExprBuilder {
	return b.makeNullCheck(astIsNotNull)
}

// Ascending creates an ASC order expression.
func (b ValExprBuilder) Ascending() *Order {
	return &Order{
		Expr:      b.ValExpr,
		Direction: " ASC",
	}
}

// Descending creates a DESC order expression.
func (b ValExprBuilder) Descending() *Order {
	return &Order{
		Expr:      b.ValExpr,
		Direction: " DESC",
	}
}

func (b ValExprBuilder) makeFunc(name string, distinct bool) ValExprBuilder {
	if !isValidIdentifier(name) {
		return ValExprBuilder{makeErrVal("invalid FUNC identifier: %s", name)}
	}

	var exprs SelectExprs
	switch t := b.ValExpr.(type) {
	case ValTuple:
		for _, e := range t.Exprs {
			exprs = append(exprs, &NonStarExpr{Expr: unwrapValExpr(e)})
		}
	default:
		exprs = SelectExprs([]SelectExpr{&NonStarExpr{Expr: b.ValExpr}})
	}
	return ValExprBuilder{
		&FuncExpr{
			Name:     name,
			Distinct: distinct,
			Exprs:    exprs,
		}}
}

// Count creates a COUNT(...) expression.
func (b ValExprBuilder) Count() ValExprBuilder {
	return b.makeFunc("COUNT", false)
}

// CountDistinct creates a COUNT(DISTINCT ...) expression.
func (b ValExprBuilder) CountDistinct() ValExprBuilder {
	return b.makeFunc("COUNT", true)
}

// Max creates a MAX(...) expression.
func (b ValExprBuilder) Max() ValExprBuilder {
	return b.makeFunc("MAX", false)
}

// Min creates a MIN(...) expression.
func (b ValExprBuilder) Min() ValExprBuilder {
	return b.makeFunc("MIN", false)
}

// Func creates a function expression where name is the name of the
// function. The function will be invoked as name(val).
func (b ValExprBuilder) Func(name string) ValExprBuilder {
	return b.makeFunc(name, false)
}

// FuncDistinct creates a function expression where name is the name
// of the function. The function will be invoked as name(DISTINCT val).
func (b ValExprBuilder) FuncDistinct(name string) ValExprBuilder {
	return b.makeFunc(name, true)
}

func (b ValExprBuilder) makeBinaryExpr(op byte, expr interface{}) ValExprBuilder {
	left := b.ValExpr
	switch left.(type) {
	case *BinaryExpr:
		left = makeValTuple([]interface{}{left})
	}
	return ValExprBuilder{
		&BinaryExpr{
			Operator: op,
			Left:     left,
			Right:    makeValExpr(expr),
		}}
}

// BitAnd creates a & expression.
func (b ValExprBuilder) BitAnd(expr interface{}) ValExprBuilder {
	return b.makeBinaryExpr('&', expr)
}

// BitOr creates a | expression.
func (b ValExprBuilder) BitOr(expr interface{}) ValExprBuilder {
	return b.makeBinaryExpr('|', expr)
}

// BitXor creates a ^ expression.
func (b ValExprBuilder) BitXor(expr interface{}) ValExprBuilder {
	return b.makeBinaryExpr('^', expr)
}

// Plus creates a + expression.
func (b ValExprBuilder) Plus(expr interface{}) ValExprBuilder {
	return b.makeBinaryExpr('+', expr)
}

// Minus creates a - expression.
func (b ValExprBuilder) Minus(expr interface{}) ValExprBuilder {
	return b.makeBinaryExpr('-', expr)
}

// Mul creates a * expression.
func (b ValExprBuilder) Mul(expr interface{}) ValExprBuilder {
	return b.makeBinaryExpr('*', expr)
}

// Div creates a / expression.
func (b ValExprBuilder) Div(expr interface{}) ValExprBuilder {
	return b.makeBinaryExpr('/', expr)
}

// Mod creates a % expression.
func (b ValExprBuilder) Mod(expr interface{}) ValExprBuilder {
	return b.makeBinaryExpr('%', expr)
}

// BoolExprBuilder aids the construction of boolean expressions from
// other boolean expressions.
type BoolExprBuilder struct {
	BoolExpr
}

func unwrapBoolExpr(expr BoolExpr) BoolExpr {
	if b, ok := expr.(BoolExprBuilder); ok {
		return b.BoolExpr
	}
	return expr
}

// And creates an AND expression.
func (e BoolExprBuilder) And(expr BoolExpr) BoolExprBuilder {
	var conditions []BoolExpr

	if andExpr, ok := e.BoolExpr.(*AndExpr); ok {
		conditions = append(conditions, andExpr.Exprs...)
	} else {
		conditions = append(conditions, e.BoolExpr)
	}

	unwrapped := unwrapBoolExpr(expr)
	if andExpr, ok := unwrapped.(*AndExpr); ok {
		conditions = append(conditions, andExpr.Exprs...)
	} else {
		conditions = append(conditions, unwrapped)
	}

	return BoolExprBuilder{
		&AndExpr{
			Op:    astAndExpr,
			Exprs: conditions,
		}}
}

// Or creates an OR expression.
func (e BoolExprBuilder) Or(expr BoolExpr) BoolExprBuilder {
	var conditions []BoolExpr

	if orExpr, ok := e.BoolExpr.(*OrExpr); ok {
		conditions = append(conditions, orExpr.Exprs...)
	} else {
		conditions = append(conditions, e.BoolExpr)
	}

	unwrapped := unwrapBoolExpr(expr)
	if orExpr, ok := unwrapped.(*OrExpr); ok {
		conditions = append(conditions, orExpr.Exprs...)
	} else {
		conditions = append(conditions, unwrapped)
	}

	return BoolExprBuilder{
		&OrExpr{
			Op:    astOrExpr,
			Exprs: conditions,
		}}
}

// Not creates a NOT expression.
func (e BoolExprBuilder) Not() BoolExprBuilder {
	return BoolExprBuilder{
		&NotExpr{
			Op:   astNotExpr,
			Expr: &ParenBoolExpr{Expr: e.BoolExpr},
		}}
}

func makeValTuple(list []interface{}) ValTuple {
	var result ValExprs
	for _, arg := range list {
		// Handle various types of slices in order to make it easier to
		// pass concrete slice types to In() and NotIn().
		switch t := arg.(type) {
		case []int:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []int16:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []int32:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []int64:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []uint:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []uint16:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []uint32:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []uint64:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []float32:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []float64:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []string:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case [][]byte:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		case []time.Time:
			for _, v := range t {
				result = append(result, makeValExpr(v))
			}
		default:
			result = append(result, makeValExpr(arg))
		}
	}
	return ValTuple{Exprs: result}
}

func makeErrVal(format string, args ...interface{}) ErrVal {
	return ErrVal{
		Err: fmt.Errorf(format, args...),
	}
}

func makeOrderBy(exprs ...interface{}) []*Order {
	var orders []*Order
	for _, e := range exprs {
		switch t := e.(type) {
		case *Order:
			orders = append(orders, t)
		case ValExpr:
			orders = append(orders, &Order{
				Expr: unwrapValExpr(t),
			})
		default:
			orders = append(orders, &Order{
				Expr: makeErrVal("unsupported type %T: %v", e, e),
			})
		}
	}
	return orders
}

func getColName(table *Table, col interface{}) *ColName {
	switch t := col.(type) {
	case string:
		return table.column(t)
	case *ColName:
		return t
	case ValExprBuilder:
		switch t := t.ValExpr.(type) {
		case *ColName:
			return t
		}
	}
	return nil
}

func makeUpdateExpr(table *Table, col interface{}, val interface{}) *UpdateExpr {
	colName := getColName(table, col)
	if colName == nil {
		colName = &ColName{Name: "#error"}
		val = makeErrVal("invalid update column: %v", col)
	}
	return &UpdateExpr{
		Name: colName,
		Expr: makeValExpr(val),
	}
}

// L creates a ValExpr from the supplied val, performing type
// conversions when possible. Val can be a builtin type like int or
// string, or an existing ValExpr such as a column returned by
// Table.C.
func L(val interface{}) ValExprBuilder {
	return ValExprBuilder{makeValExpr(val)}
}

// G creates a group of values.
func G(list ...interface{}) ValExprBuilder {
	if len(list) == 0 {
		return ValExprBuilder{makeErrVal("empty group")}
	}
	return ValExprBuilder{makeValTuple(list)}
}

// This is a strict subset of the actual restrictions
var identifierRE = regexp.MustCompile("^[a-zA-Z_]\\w*$")

// Returns true if the given string is suitable as an identifier.
func isValidIdentifier(name string) bool {
	return identifierRE.MatchString(name)
}
