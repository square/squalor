// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// SQUARE NOTE: The encoding routines were derived from vitess's
// sqlparser package. The original source can be found at
// https://code.google.com/p/vitess/

package squalor

import (
	"bytes"
	"fmt"
	"io"
)

// Instructions for creating new types: If a type needs to satisfy an
// interface, declare that function along with that interface. This
// will help users identify the list of types to which they can assert
// those interfaces. If the member of a type has a string with a
// predefined list of values, declare those values as const following
// the type. For interfaces that define dummy functions to
// consolidate a set of types, define the function as typeName().
// This will help avoid name collisions.

// The Serializer interface is implemented by all
// expressions/statements.
type Serializer interface {
	// Serialize writes the statement/expression to the Writer. If an
	// error is returned the Writer may contain partial output.
	Serialize(w Writer) error
}

// Serialize serializes a serializer to a string.
func Serialize(s Serializer) (string, error) {
	w := &standardWriter{}
	if err := s.Serialize(w); err != nil {
		return "", err
	}
	return w.String(), nil
}

// SerializeWithPlaceholders serializes a serializer to a string but without substituting
// values. It may be useful for logging.
func SerializeWithPlaceholders(s Serializer) (string, error) {
	w := &placeholderWriter{}
	if err := s.Serialize(w); err != nil {
		return "", err
	}
	return w.String(), nil
}

// Writer defines an interface for writing a AST as SQL.
type Writer interface {
	io.Writer

	// WriteBytes writes a string of unprintable value.
	WriteBytes(node BytesVal) error
	// WriteEncoded writes an already encoded value.
	WriteEncoded(node EncodedVal) error
	// WriteNum writes a number value.
	WriteNum(node NumVal) error
	// WriteRaw writes a raw Go value.
	WriteRaw(node RawVal) error
	// WriteStr writes a SQL string value.
	WriteStr(node StrVal) error
}

type standardWriter struct {
	bytes.Buffer
}

func (w *standardWriter) WriteRaw(node RawVal) error {
	return encodeSQLValue(w, node.Val)
}

func (w *standardWriter) WriteEncoded(node EncodedVal) error {
	_, err := w.Write(node.Val)
	return err
}

func (w *standardWriter) WriteStr(node StrVal) error {
	return encodeSQLString(w, string(node))
}

func (w *standardWriter) WriteBytes(node BytesVal) error {
	return encodeSQLBytes(w, []byte(node))
}

func (w *standardWriter) WriteNum(node NumVal) error {
	_, err := io.WriteString(w, string(node))
	return err
}

// placeholderWriter will write all SQL value types as ? placeholders.
type placeholderWriter struct {
	bytes.Buffer
}

func (w *placeholderWriter) WriteRaw(node RawVal) error {
	_, err := w.Write(astPlaceholder)
	return err
}

func (w *placeholderWriter) WriteEncoded(node EncodedVal) error {
	_, err := w.Write(astPlaceholder)
	return err
}

func (w *placeholderWriter) WriteStr(node StrVal) error {
	_, err := w.Write(astPlaceholder)
	return err
}

func (w *placeholderWriter) WriteBytes(node BytesVal) error {
	_, err := w.Write(astPlaceholder)
	return err
}

func (w *placeholderWriter) WriteNum(node NumVal) error {
	_, err := w.Write(astPlaceholder)
	return err
}

var (
	// Placeholder is a placeholder for a value in a SQL statement. It is replaced with
	// an actual value when the query is executed.
	Placeholder = PlaceholderVal{}
)

// Statement represents a statement.
type Statement interface {
	Serializer
	statement()
}

func (*Union) statement()  {}
func (*Select) statement() {}
func (*Insert) statement() {}
func (*Update) statement() {}
func (*Delete) statement() {}

// SelectStatement any SELECT statement.
type SelectStatement interface {
	Statement
	selectStatement()
	insertRows()
}

func (*Select) selectStatement() {}
func (*Union) selectStatement()  {}

// Select represents a SELECT statement.
type Select struct {
	Comments Comments
	Distinct string
	Exprs    SelectExprs
	From     TableExprs
	Where    *Where
	GroupBy  GroupBy
	Having   *Where
	OrderBy  OrderBy
	Limit    *Limit
	Lock     string
}

// Select.Distinct
const (
	astDistinct = "DISTINCT "
)

// Select.Lock
const (
	astForUpdate = " FOR UPDATE"
	astShareMode = " LOCK IN SHARE MODE"
)

var (
	astSelect     = []byte("SELECT ")
	astSelectFrom = []byte(" FROM ")
)

func (node *Select) Serialize(w Writer) error {
	if _, err := w.Write(astSelect); err != nil {
		return err
	}
	if _, err := io.WriteString(w, node.Distinct); err != nil {
		return err
	}
	if err := node.Exprs.Serialize(w); err != nil {
		return err
	}
	if _, err := w.Write(astSelectFrom); err != nil {
		return err
	}
	if err := node.From.Serialize(w); err != nil {
		return err
	}
	if err := node.Where.Serialize(w); err != nil {
		return err
	}
	if err := node.GroupBy.Serialize(w); err != nil {
		return err
	}
	if err := node.Having.Serialize(w); err != nil {
		return err
	}
	if err := node.OrderBy.Serialize(w); err != nil {
		return err
	}
	if err := node.Limit.Serialize(w); err != nil {
		return err
	}
	_, err := io.WriteString(w, node.Lock)
	return err
}

// Union represents a UNION statement.
type Union struct {
	Type        string
	Left, Right SelectStatement
}

// Union.Type
const (
	astUnion     = "UNION"
	astUnionAll  = "UNION ALL"
	astSetMinus  = "MINUS"
	astExcept    = "EXCEPT"
	astIntersect = "INTERSECT"
)

func (node *Union) Serialize(w Writer) error {
	if err := node.Left.Serialize(w); err != nil {
		return err
	}
	if _, err := w.Write(astSpace); err != nil {
		return err
	}
	if _, err := io.WriteString(w, node.Type); err != nil {
		return err
	}
	if _, err := w.Write(astSpace); err != nil {
		return err
	}
	return node.Right.Serialize(w)
}

// Insert represents an INSERT or REPLACE statement.
type Insert struct {
	Kind     string
	Comments Comments
	Table    *TableName
	Columns  Columns
	Rows     InsertRows
	OnDup    OnDup
}

var (
	astInsertInto = []byte("INTO ")
	astSpace      = []byte(" ")
)

func (node *Insert) Serialize(w Writer) error {
	if _, err := io.WriteString(w, node.Kind); err != nil {
		return err
	}
	if _, err := w.Write(astSpace); err != nil {
		return err
	}
	if err := node.Comments.Serialize(w); err != nil {
		return err
	}
	if _, err := w.Write(astInsertInto); err != nil {
		return err
	}
	if err := node.Table.Serialize(w); err != nil {
		return err
	}
	if _, err := w.Write(astSpace); err != nil {
		return err
	}
	if err := node.Columns.Serialize(w); err != nil {
		return err
	}
	if _, err := w.Write(astSpace); err != nil {
		return err
	}
	if err := node.Rows.Serialize(w); err != nil {
		return err
	}
	return node.OnDup.Serialize(w)
}

// InsertRows represents the rows for an INSERT statement.
type InsertRows interface {
	Serializer
	insertRows()
}

func (*Select) insertRows() {}
func (*Union) insertRows()  {}
func (Values) insertRows()  {}

// Update represents an UPDATE statement.
type Update struct {
	Comments Comments
	Table    *TableName
	Tables   []*Table
	Exprs    UpdateExprs
	Where    *Where
	OrderBy  OrderBy
	Limit    *Limit
}

var (
	astUpdate = []byte("UPDATE ")
	astSet    = []byte(" SET ")
)

func (node *Update) Serialize(w Writer) error {
	if _, err := w.Write(astUpdate); err != nil {
		return err
	}
	if err := node.Comments.Serialize(w); err != nil {
		return err
	}
	if err := node.Table.Serialize(w); err != nil {
		return err
	}
	if _, err := w.Write(astSet); err != nil {
		return err
	}
	if err := node.Exprs.Serialize(w); err != nil {
		return err
	}
	if err := node.Where.Serialize(w); err != nil {
		return err
	}
	if err := node.OrderBy.Serialize(w); err != nil {
		return err
	}
	return node.Limit.Serialize(w)
}

// Delete represents a DELETE statement.
type Delete struct {
	Comments Comments
	// Table is either a TableName or a JoinBuilder
	Table abstractTable
	// TableNames is a list of tables to perform the delete on. This is only necessary when doing
	// joins, because you may not want to delete from all the tables involved in the join.
	// In deletes without joins, this may be empty or the table being deleted from.
	TableNames TableNames
	Where      *Where
	OrderBy    OrderBy
	Limit      *Limit
}

var (
	astDelete     = []byte("DELETE ")
	astDeleteFrom = []byte("FROM ")
)

func (node *Delete) Serialize(w Writer) error {
	if _, err := w.Write(astDelete); err != nil {
		return err
	}
	if err := node.Comments.Serialize(w); err != nil {
		return err
	}
	if len(node.TableNames) != 0 {
		if err := node.TableNames.Serialize(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, " "); err != nil {
			return err
		}
	}
	if _, err := w.Write(astDeleteFrom); err != nil {
		return err
	}
	if err := node.Table.tableExpr().Serialize(w); err != nil {
		return err
	}
	if err := node.Where.Serialize(w); err != nil {
		return err
	}
	if err := node.OrderBy.Serialize(w); err != nil {
		return err
	}
	return node.Limit.Serialize(w)
}

// Comments represents a list of comments.
type Comments []string

func (node Comments) Serialize(w Writer) error {
	for _, c := range node {
		if _, err := io.WriteString(w, c); err != nil {
			return nil
		}
		if _, err := w.Write(astSpace); err != nil {
			return nil
		}
	}
	return nil
}

// TableNames represents several table names. It is used in deletes that have joins.
type TableNames []*TableName

func (node TableNames) Serialize(w Writer) error {
	var prefix []byte
	for _, n := range node {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if err := n.Serialize(w); err != nil {
			return err
		}
		prefix = astCommaSpace
	}
	return nil
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

var (
	astCommaSpace = []byte(", ")
)

func (node SelectExprs) Serialize(w Writer) error {
	var prefix []byte
	for _, n := range node {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if err := n.Serialize(w); err != nil {
			return err
		}
		prefix = astCommaSpace
	}
	return nil
}

// SelectExpr represents a SELECT expression.
type SelectExpr interface {
	Serializer
	selectExpr()
}

func (*StarExpr) selectExpr()    {}
func (*NonStarExpr) selectExpr() {}

// StarExpr defines a '*' or 'table.*' expression.
type StarExpr struct {
	TableName string
}

var (
	astStar = []byte("*")
)

func (node *StarExpr) Serialize(w Writer) error {
	if node.TableName != "" {
		if err := quoteName(w, node.TableName); err != nil {
			return err
		}
		if _, err := w.Write(astPeriod); err != nil {
			return err
		}
	}
	_, err := w.Write(astStar)
	return err
}

// NonStarExpr defines a non-'*' select expr.
type NonStarExpr struct {
	Expr Expr
	As   string
}

var (
	astAsPrefix = []byte(" AS `")
)

func (node *NonStarExpr) Serialize(w Writer) error {
	if err := node.Expr.Serialize(w); err != nil {
		return err
	}
	if node.As != "" {
		if _, err := w.Write(astAsPrefix); err != nil {
			return err
		}
		if _, err := io.WriteString(w, node.As); err != nil {
			return err
		}
		if _, err := w.Write(astBackquote); err != nil {
			return err
		}
	}
	return nil
}

// Columns represents an insert column list.
// The syntax for Columns is a subset of SelectExprs.
// So, it's castable to a SelectExprs and can be analyzed
// as such.
type Columns []SelectExpr

var (
	astOpenParen  = []byte("(")
	astCloseParen = []byte(")")
)

func (node Columns) Serialize(w Writer) error {
	if _, err := w.Write(astOpenParen); err != nil {
		return err
	}
	if err := SelectExprs(node).Serialize(w); err != nil {
		return err
	}
	_, err := w.Write(astCloseParen)
	return err
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

func (node TableExprs) Serialize(w Writer) error {
	var prefix []byte
	for _, n := range node {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if err := n.Serialize(w); err != nil {
			return err
		}
		prefix = astCommaSpace
	}
	return nil
}

// TableExpr represents a table expression.
type TableExpr interface {
	Serializer
	tableExpr()
}

func (*AliasedTableExpr) tableExpr() {}
func (*ParenTableExpr) tableExpr()   {}
func (*JoinTableExpr) tableExpr()    {}

// AliasedTableExpr represents a table expression
// coupled with an optional alias or index hint.
type AliasedTableExpr struct {
	Expr  SimpleTableExpr
	As    string
	Hints *IndexHints
}

func (node *AliasedTableExpr) Serialize(w Writer) error {
	if err := node.Expr.Serialize(w); err != nil {
		return err
	}
	if node.As != "" {
		if _, err := w.Write(astAsPrefix); err != nil {
			return err
		}
		if _, err := io.WriteString(w, node.As); err != nil {
			return err
		}
		if _, err := w.Write(astBackquote); err != nil {
			return err
		}
	}
	if node.Hints != nil {
		// Hint node provides the space padding.
		if err := node.Hints.Serialize(w); err != nil {
			return err
		}
	}
	return nil
}

// SimpleTableExpr represents a simple table expression.
type SimpleTableExpr interface {
	Serializer
	simpleTableExpr()
}

func (*TableName) simpleTableExpr() {}
func (*Subquery) simpleTableExpr()  {}

// TableName represents a table  name.
type TableName struct {
	Name, Qualifier string
}

func (node *TableName) Serialize(w Writer) error {
	if node.Qualifier != "" {
		if err := quoteName(w, node.Qualifier); err != nil {
			return err
		}
		if _, err := w.Write(astPeriod); err != nil {
			return err
		}
	}
	return quoteName(w, node.Name)
}

// ParenTableExpr represents a parenthesized TableExpr.
type ParenTableExpr struct {
	Expr TableExpr
}

// JoinTableExpr represents a TableExpr that's a JOIN operation.
type JoinTableExpr struct {
	LeftExpr  TableExpr
	Join      string
	RightExpr TableExpr
	Cond      JoinCond
}

// JoinTableExpr.Join
const (
	astJoin         = "JOIN"
	astStraightJoin = "STRAIGHT_JOIN"
	astLeftJoin     = "LEFT JOIN"
	astRightJoin    = "RIGHT JOIN"
	astCrossJoin    = "CROSS JOIN"
	astNaturalJoin  = "NATURAL JOIN"
)

func (node *JoinTableExpr) Serialize(w Writer) error {
	if err := node.LeftExpr.Serialize(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, " %s ", node.Join); err != nil {
		return err
	}
	if err := node.RightExpr.Serialize(w); err != nil {
		return err
	}
	if node.Cond != nil {
		if err := node.Cond.Serialize(w); err != nil {
			return err
		}
	}
	return nil
}

// JoinCond represents a join condition.
type JoinCond interface {
	Serializer
	joinCond()
}

func (*OnJoinCond) joinCond()    {}
func (*UsingJoinCond) joinCond() {}

// OnJoinCond represents an ON join condition.
type OnJoinCond struct {
	Expr BoolExpr
}

var (
	astOn = []byte(" ON ")
)

func (node *OnJoinCond) Serialize(w Writer) error {
	if _, err := w.Write(astOn); err != nil {
		return err
	}
	return node.Expr.Serialize(w)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols Columns
}

var (
	astUsing = []byte(" USING ")
)

func (node *UsingJoinCond) Serialize(w Writer) error {
	if _, err := w.Write(astUsing); err != nil {
		return err
	}
	return node.Cols.Serialize(w)
}

// IndexHints represents a list of index hints.
type IndexHints struct {
	Type    string
	Indexes []string
}

const (
	astUse    = "USE"
	astIgnore = "IGNORE"
	astForce  = "FORCE"
)

func (node *IndexHints) Serialize(w Writer) error {
	if _, err := fmt.Fprintf(w, " %s INDEX ", node.Type); err != nil {
		return err
	}
	prefix := "("
	for _, n := range node.Indexes {
		if _, err := fmt.Fprintf(w, "%s%s", prefix, n); err != nil {
			return err
		}
		prefix = ", "
	}
	_, err := fmt.Fprintf(w, ")")
	return err
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr BoolExpr
}

// Where.Type
const (
	astWhere  = " WHERE "
	astHaving = " HAVING "
)

// NewWhere creates a WHERE or HAVING clause out
// of a BoolExpr. If the expression is nil, it returns nil.
func NewWhere(typ string, expr BoolExpr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

func (node *Where) Serialize(w Writer) error {
	if node == nil {
		return nil
	}
	if _, err := io.WriteString(w, node.Type); err != nil {
		return err
	}
	return node.Expr.Serialize(w)
}

// Expr represents an expression.
type Expr interface {
	Serializer
	expr()
}

func (*AndExpr) expr()        {}
func (*OrExpr) expr()         {}
func (*NotExpr) expr()        {}
func (*ParenBoolExpr) expr()  {}
func (*ComparisonExpr) expr() {}
func (*RangeCond) expr()      {}
func (*NullCheck) expr()      {}
func (*ExistsExpr) expr()     {}
func (PlaceholderVal) expr()  {}
func (RawVal) expr()          {}
func (EncodedVal) expr()      {}
func (StrVal) expr()          {}
func (NumVal) expr()          {}
func (ValArg) expr()          {}
func (*NullVal) expr()        {}
func (*ColName) expr()        {}
func (ValTuple) expr()        {}
func (*Subquery) expr()       {}
func (*BinaryExpr) expr()     {}
func (*UnaryExpr) expr()      {}
func (*FuncExpr) expr()       {}
func (*CaseExpr) expr()       {}

// BoolExpr represents a boolean expression.
type BoolExpr interface {
	boolExpr()
	Expr
}

func (*AndExpr) boolExpr()        {}
func (*OrExpr) boolExpr()         {}
func (*NotExpr) boolExpr()        {}
func (*ParenBoolExpr) boolExpr()  {}
func (*ComparisonExpr) boolExpr() {}
func (*RangeCond) boolExpr()      {}
func (*NullCheck) boolExpr()      {}
func (*ExistsExpr) boolExpr()     {}

const (
	astAndExpr = " AND "
)

// AndExpr represents an AND expression.
type AndExpr struct {
	Op    string
	Exprs []BoolExpr
}

func (node *AndExpr) Serialize(w Writer) error {
	if len(node.Exprs) == 0 {
		_, err := w.Write(astBoolTrue)
		return err
	} else if len(node.Exprs) == 1 {
		return node.Exprs[0].Serialize(w)
	}

	if _, err := w.Write(astOpenParen); err != nil {
		return err
	}
	if err := node.Exprs[0].Serialize(w); err != nil {
		return err
	}
	for _, expr := range node.Exprs[1:] {
		if _, err := io.WriteString(w, node.Op); err != nil {
			return err
		}
		if err := expr.Serialize(w); err != nil {
			return err
		}
	}
	_, err := w.Write(astCloseParen)
	return err
}

const (
	astOrExpr = " OR "
)

// OrExpr represents an OR expression.
type OrExpr struct {
	Op    string
	Exprs []BoolExpr
}

func (node *OrExpr) Serialize(w Writer) error {
	if len(node.Exprs) == 0 {
		_, err := w.Write(astBoolFalse)
		return err
	} else if len(node.Exprs) == 1 {
		return node.Exprs[0].Serialize(w)
	}

	if _, err := w.Write(astOpenParen); err != nil {
		return err
	}
	if err := node.Exprs[0].Serialize(w); err != nil {
		return err
	}
	for _, expr := range node.Exprs[1:] {
		if _, err := io.WriteString(w, node.Op); err != nil {
			return err
		}
		if err := expr.Serialize(w); err != nil {
			return err
		}
	}
	_, err := w.Write(astCloseParen)
	return err
}

const (
	astNotExpr = "NOT "
)

// NotExpr represents a NOT expression.
type NotExpr struct {
	Op   string
	Expr BoolExpr
}

func (node *NotExpr) Serialize(w Writer) error {
	if _, err := io.WriteString(w, node.Op); err != nil {
		return err
	}
	return node.Expr.Serialize(w)
}

// ParenBoolExpr represents a parenthesized boolean expression.
type ParenBoolExpr struct {
	Expr BoolExpr
}

func (node *ParenBoolExpr) Serialize(w Writer) error {
	if _, err := w.Write(astOpenParen); err != nil {
		return err
	}
	if err := node.Expr.Serialize(w); err != nil {
		return err
	}
	_, err := w.Write(astCloseParen)
	return err
}

// ComparisonExpr represents a two-value comparison expression.
type ComparisonExpr struct {
	Operator    string
	Left, Right ValExpr
}

// ComparisonExpr.Operator
const (
	astEQ        = " = "
	astLT        = " < "
	astGT        = " > "
	astLE        = " <= "
	astGE        = " >= "
	astNE        = " != "
	astNSE       = " <=> "
	astIn        = " IN "
	astNot       = " NOT "
	astNotIn     = " NOT IN "
	astLike      = " LIKE "
	astNotLike   = " NOT LIKE "
	astRegExp    = " REGEXP "
	astNotRegExp = " NOT REGEXP "
)

func (node *ComparisonExpr) Serialize(w Writer) error {
	if err := node.Left.Serialize(w); err != nil {
		return err
	}
	if _, err := io.WriteString(w, node.Operator); err != nil {
		return err
	}
	return node.Right.Serialize(w)
}

// RangeCond represents a BETWEEN or a NOT BETWEEN expression.
type RangeCond struct {
	Operator string
	Left     ValExpr
	From, To ValExpr
}

// RangeCond.Operator
const (
	astBetween    = " BETWEEN "
	astNotBetween = " NOT BETWEEN "
)

var (
	astAnd = []byte(" AND ")
)

func (node *RangeCond) Serialize(w Writer) error {
	if err := node.Left.Serialize(w); err != nil {
		return err
	}
	if _, err := io.WriteString(w, node.Operator); err != nil {
		return err
	}
	if err := node.From.Serialize(w); err != nil {
		return err
	}
	if _, err := w.Write(astAnd); err != nil {
		return err
	}
	return node.To.Serialize(w)
}

// NullCheck represents an IS NULL or an IS NOT NULL expression.
type NullCheck struct {
	Operator string
	Expr     ValExpr
}

// NullCheck.Operator
const (
	astIsNull    = " IS NULL"
	astIsNotNull = " IS NOT NULL"
)

func (node *NullCheck) Serialize(w Writer) error {
	if err := node.Expr.Serialize(w); err != nil {
		return err
	}
	_, err := io.WriteString(w, node.Operator)
	return err
}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Subquery *Subquery
}

// ValExpr represents a value expression.
type ValExpr interface {
	valExpr()
	Expr
}

func (PlaceholderVal) valExpr() {}
func (RawVal) valExpr()         {}
func (EncodedVal) valExpr()     {}
func (StrVal) valExpr()         {}
func (NumVal) valExpr()         {}
func (ValArg) valExpr()         {}
func (*NullVal) valExpr()       {}
func (*ColName) valExpr()       {}
func (ValTuple) valExpr()       {}
func (*Subquery) valExpr()      {}
func (*BinaryExpr) valExpr()    {}
func (*UnaryExpr) valExpr()     {}
func (*FuncExpr) valExpr()      {}
func (*CaseExpr) valExpr()      {}

var (
	astPlaceholder = []byte("?")
)

// PlaceholderVal represents a placeholder parameter that will be supplied
// when executing the query. It will be serialized as a ?.
type PlaceholderVal struct{}

func (node PlaceholderVal) Serialize(w Writer) error {
	_, err := w.Write(astPlaceholder)
	return err
}

// RawVal represents a raw go value
type RawVal struct {
	Val interface{}
}

var (
	astBoolTrue  = []byte("1")
	astBoolFalse = []byte("0")
)

func (node RawVal) Serialize(w Writer) error {
	return w.WriteRaw(node)
}

// EncodedVal represents an already encoded value. This struct must be used
// with caution because misuse can provide an avenue for SQL injection attacks.
type EncodedVal struct {
	Val []byte
}

func (node EncodedVal) Serialize(w Writer) error {
	return w.WriteEncoded(node)
}

// StrVal represents a string value.
type StrVal string

func (node StrVal) Serialize(w Writer) error {
	return w.WriteStr(node)
}

// BytesVal represents a string of unprintable value.
type BytesVal []byte

func (BytesVal) expr()    {}
func (BytesVal) valExpr() {}

func (node BytesVal) Serialize(w Writer) error {
	return w.WriteBytes(node)
}

// ErrVal represents an error condition that occurred while
// constructing a tree.
type ErrVal struct {
	Err error
}

func (ErrVal) expr()    {}
func (ErrVal) valExpr() {}

func (node ErrVal) Serialize(w Writer) error {
	return node.Err
}

// NumVal represents a number.
type NumVal string

func (node NumVal) Serialize(w Writer) error {
	return w.WriteNum(node)
}

// ValArg represents a named bind var argument.
type ValArg string

func (node ValArg) Serialize(w Writer) error {
	_, err := fmt.Fprintf(w, ":%s", string(node)[1:])
	return err
}

// NullVal represents a NULL value.
type NullVal struct{}

var (
	astNull = []byte("NULL")
)

func (node *NullVal) Serialize(w Writer) error {
	_, err := w.Write(astNull)
	return err
}

// ColName represents a column name.
type ColName struct {
	Name, Qualifier string
}

var (
	astBackquote = []byte("`")
	astPeriod    = []byte(".")
)

func (node *ColName) Serialize(w Writer) error {
	if node.Qualifier != "" {
		if err := quoteName(w, node.Qualifier); err != nil {
			return err
		}
		if _, err := w.Write(astPeriod); err != nil {
			return err
		}
	}
	return quoteName(w, node.Name)
}

// note: quoteName does not escape s. quoteName is indirectly
// called by builder.go, which checks that column/table names exist.
func quoteName(w io.Writer, s string) error {
	if _, err := w.Write(astBackquote); err != nil {
		return err
	}
	if _, err := io.WriteString(w, s); err != nil {
		return err
	}
	_, err := w.Write(astBackquote)
	return err
}

// Tuple represents a tuple. It can be ValTuple, Subquery.
type Tuple interface {
	tuple()
	ValExpr
}

func (ValTuple) tuple()  {}
func (*Subquery) tuple() {}

// ValTuple represents a tuple of actual values.
type ValTuple struct {
	Exprs ValExprs
}

func (node ValTuple) Serialize(w Writer) error {
	if _, err := w.Write(astOpenParen); err != nil {
		return err
	}
	if err := node.Exprs.Serialize(w); err != nil {
		return err
	}
	_, err := w.Write(astCloseParen)
	return err
}

// ValExprs represents a list of value expressions.
// It's not a valid expression because it's not parenthesized.
type ValExprs []ValExpr

func (node ValExprs) Serialize(w Writer) error {
	var prefix []byte
	for _, n := range node {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if err := n.Serialize(w); err != nil {
			return err
		}
		prefix = astCommaSpace
	}
	return nil
}

// Subquery represents a subquery.
type Subquery struct {
	Select SelectStatement
}

func (s *Subquery) Serialize(w Writer) error {
	_, err := w.Write([]byte{'('})
	if err != nil {
		return err
	}
	err = s.Select.Serialize(w)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte{')'})
	if err != nil {
		return err
	}
	return nil
}

// BinaryExpr represents a binary value expression.
type BinaryExpr struct {
	Operator    byte
	Left, Right Expr
}

// BinaryExpr.Operator
const (
	astBitand = '&'
	astBitor  = '|'
	astBitxor = '^'
	astPlus   = '+'
	astMinus  = '-'
	astMult   = '*'
	astDiv    = '/'
	astMod    = '%'
)

func (node *BinaryExpr) Serialize(w Writer) error {
	if err := node.Left.Serialize(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%c", node.Operator); err != nil {
		return err
	}
	return node.Right.Serialize(w)
}

// UnaryExpr represents a unary value expression.
type UnaryExpr struct {
	Operator byte
	Expr     Expr
}

// UnaryExpr.Operator
const (
	astUnaryPlus  = '+'
	astUnaryMinus = '-'
	astTilda      = '~'
)

func (node *UnaryExpr) Serialize(w Writer) error {
	if _, err := fmt.Fprintf(w, "%c", node.Operator); err != nil {
		return err
	}
	return node.Expr.Serialize(w)
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Name     string
	Distinct bool
	Exprs    SelectExprs
}

var (
	astFuncDistinct = []byte("DISTINCT ")
)

func (node *FuncExpr) Serialize(w Writer) error {
	if _, err := io.WriteString(w, node.Name); err != nil {
		return err
	}
	if _, err := w.Write(astOpenParen); err != nil {
		return err
	}
	if node.Distinct {
		if _, err := w.Write(astFuncDistinct); err != nil {
			return err
		}
	}
	if err := node.Exprs.Serialize(w); err != nil {
		return err
	}
	_, err := w.Write(astCloseParen)
	return err
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr  ValExpr
	Whens []*When
	Else  ValExpr
}

func (node *CaseExpr) Serialize(w Writer) error {
	if _, err := fmt.Fprint(w, "CASE "); err != nil {
		return err
	}
	if node.Expr != nil {
		if err := node.Expr.Serialize(w); err != nil {
			return err
		}
		if _, err := fmt.Fprint(w, " "); err != nil {
			return err
		}
	}
	for _, when := range node.Whens {
		if err := when.Serialize(w); err != nil {
			return err
		}
		if _, err := fmt.Fprint(w, " "); err != nil {
			return err
		}
	}
	if node.Else != nil {
		if _, err := fmt.Fprint(w, "ELSE "); err != nil {
			return err
		}
		if err := node.Else.Serialize(w); err != nil {
			return err
		}
		if _, err := fmt.Fprint(w, " "); err != nil {
			return err
		}
	}
	_, err := fmt.Fprint(w, "END")
	return err
}

// When represents a WHEN sub-expression.
type When struct {
	Cond BoolExpr
	Val  ValExpr
}

func (node *When) Serialize(w Writer) error {
	if err := node.Cond.Serialize(w); err != nil {
		return err
	}
	return node.Val.Serialize(w)
}

// Values represents a VALUES clause.
type Values []Tuple

var (
	astValues = []byte("VALUES ")
)

func (node Values) Serialize(w Writer) error {
	prefix := astValues
	for _, n := range node {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if err := n.Serialize(w); err != nil {
			return err
		}
		prefix = astCommaSpace
	}
	return nil
}

// GroupBy represents a GROUP BY clause.
type GroupBy []ValExpr

var (
	astGroupBy = []byte(" GROUP BY ")
)

func (node GroupBy) Serialize(w Writer) error {
	prefix := astGroupBy
	for _, n := range node {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if err := n.Serialize(w); err != nil {
			return err
		}
		prefix = astCommaSpace
	}
	return nil
}

// OrderBy represents an ORDER By clause.
type OrderBy []*Order

var (
	astOrderBy = []byte(" ORDER BY ")
)

func (node OrderBy) Serialize(w Writer) error {
	prefix := astOrderBy
	for _, n := range node {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if err := n.Serialize(w); err != nil {
			return err
		}
		prefix = astCommaSpace
	}
	return nil
}

// Order represents an ordering expression.
type Order struct {
	Expr      ValExpr
	Direction string
}

// Order.Direction
const (
	astAsc  = " ASC"
	astDesc = " DESC"
)

func (node *Order) Serialize(w Writer) error {
	if err := node.Expr.Serialize(w); err != nil {
		return err
	}
	_, err := io.WriteString(w, node.Direction)
	return err
}

// Limit represents a LIMIT clause.
type Limit struct {
	Offset, Rowcount ValExpr
}

var (
	astLimit = []byte(" LIMIT ")
)

func (node *Limit) Serialize(w Writer) error {
	if node == nil {
		return nil
	}
	if _, err := w.Write(astLimit); err != nil {
		return err
	}
	if node.Offset != nil {
		if err := node.Offset.Serialize(w); err != nil {
			return err
		}
		if _, err := w.Write(astCommaSpace); err != nil {
			return err
		}
	}
	return node.Rowcount.Serialize(w)
}

// UpdateExprs represents a list of update expressions.
type UpdateExprs []*UpdateExpr

func (node UpdateExprs) Serialize(w Writer) error {
	var prefix []byte
	for _, n := range node {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if err := n.Serialize(w); err != nil {
			return err
		}
		prefix = astCommaSpace
	}
	return nil
}

// UpdateExpr represents an update expression.
type UpdateExpr struct {
	Name *ColName
	Expr ValExpr
}

var (
	astUpdateEq = []byte(" = ")
)

func (node *UpdateExpr) Serialize(w Writer) error {
	if err := node.Name.Serialize(w); err != nil {
		return nil
	}
	if _, err := w.Write(astUpdateEq); err != nil {
		return nil
	}
	return node.Expr.Serialize(w)
}

// OnDup represents an ON DUPLICATE KEY clause.
type OnDup UpdateExprs

var (
	astOnDupKeyUpdate = []byte(" ON DUPLICATE KEY UPDATE ")
)

func (node OnDup) Serialize(w Writer) error {
	if node == nil {
		return nil
	}
	if _, err := w.Write(astOnDupKeyUpdate); err != nil {
		return err
	}
	return UpdateExprs(node).Serialize(w)
}
