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
	"testing"
	"time"
)

func TestDeleteBuilder(t *testing.T) {
	type User struct {
		Foo, Bar, Qux string
	}
	users := NewTable("users", User{})
	foo := users.C("foo")

	type Object struct {
		Foo, Baz string
	}
	objects := NewTable("objects", &Object{})
	baz := objects.C("baz")

	testCases := []struct {
		builder  *DeleteBuilder
		expected string
	}{
		{users.Delete(),
			"DELETE FROM `users`"},
		{users.Delete(users),
			"DELETE `users` FROM `users`"},
		// Where
		{users.Delete().Where(foo.Eq("bar")),
			"DELETE FROM `users` WHERE `users`.`foo` = 'bar'"},
		{users.Delete().Where(foo.In("bar", "qux")),
			"DELETE FROM `users` WHERE `users`.`foo` IN ('bar', 'qux')"},
		// OrderBy
		{users.Delete().OrderBy(foo),
			"DELETE FROM `users` ORDER BY `users`.`foo`"},
		// Limit
		{users.Delete().Limit(10),
			"DELETE FROM `users` LIMIT 10"},
		// Joins
		{users.InnerJoin(objects).Delete(),
			"DELETE FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).Delete(users),
			"DELETE `users` FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).Delete(users, objects),
			"DELETE `users`, `objects` FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).On(foo.Eq(baz)).Delete(users),
			"DELETE `users` FROM `users` INNER JOIN `objects` ON `users`.`foo` = `objects`.`baz`"},
		{users.InnerJoin(objects).On(foo.Eq(objects.C("foo"))).Delete(users),
			"DELETE `users` FROM `users` INNER JOIN `objects` ON `users`.`foo` = `objects`.`foo`"},
		{users.InnerJoin(objects).Using(foo).Delete(users),
			"DELETE `users` FROM `users` INNER JOIN `objects` USING (`foo`)"},
		{users.InnerJoin(objects).Using("foo").Delete(users),
			"DELETE `users` FROM `users` INNER JOIN `objects` USING (`foo`)"},
		{users.LeftJoin(objects).Delete(users),
			"DELETE `users` FROM `users` LEFT JOIN `objects`"},
		{users.RightJoin(objects).Delete(users),
			"DELETE `users` FROM `users` RIGHT JOIN `objects`"},
		// Subquery
		{users.Delete().Where(foo.InTuple(&Subquery{objects.Select(objects.C("foo")).Where(objects.C("foo").Gt(10))})),
			"DELETE FROM `users` WHERE `users`.`foo` IN (SELECT `objects`.`foo` FROM `objects` WHERE `objects`.`foo` > 10)"},
	}

	for _, c := range testCases {
		if sql, err := Serialize(c.builder); err != nil {
			t.Errorf("Expected success, but found %s\n%s", err, c.expected)
		} else if c.expected != sql {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expected, sql)
		}
	}
}

func TestDeleteBuilderErrors(t *testing.T) {
	type User struct {
		Foo string
	}
	users := NewTable("users", User{})

	testCases := []struct {
		builder       *DeleteBuilder
		expectedError string
	}{
		{users.Delete().Where(users.C("bar").Eq("foo")),
			"unknown column: bar",
		},
	}

	for _, c := range testCases {
		if _, err := Serialize(c.builder); err == nil {
			t.Error("Expected error, but found success")
		} else if c.expectedError != err.Error() {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expectedError, err)
		}
	}
}

func TestInsertBuilder(t *testing.T) {
	type User struct {
		Foo, Bar, Qux string
	}
	users := NewTable("users", User{})
	foo := users.C("foo")

	testCases := []struct {
		builder  *InsertBuilder
		expected string
	}{
		{users.Insert(foo).Add("bar"),
			"INSERT INTO `users` (`foo`) VALUES ('bar')"},
		{users.Insert(foo).Add(nil),
			"INSERT INTO `users` (`foo`) VALUES (NULL)"},
		{users.Insert(foo).Add([]byte(nil)),
			"INSERT INTO `users` (`foo`) VALUES (NULL)"},
		{users.Insert(foo).Add([]byte{}),
			"INSERT INTO `users` (`foo`) VALUES (X'')"},
		{users.Insert(foo).Add("bar").Add("qux"),
			"INSERT INTO `users` (`foo`) VALUES ('bar'), ('qux')"},
		{users.Insert("foo", "bar").Add("qux", 2),
			"INSERT INTO `users` (`foo`, `bar`) VALUES ('qux', 2)"},
		{users.Insert("foo").Add("bar").OnDupKeyUpdate("bar", 2),
			"INSERT INTO `users` (`foo`) VALUES ('bar') ON DUPLICATE KEY UPDATE `users`.`bar` = 2"},
		{users.Insert("foo").Add("bar").OnDupKeyUpdateColumn("bar"),
			"INSERT INTO `users` (`foo`) VALUES ('bar') ON DUPLICATE KEY UPDATE `users`.`bar` = VALUES(`users`.`bar`)"},
		{users.Insert("foo").Add("bar").OnDupKeyUpdateColumn(users.C("bar")),
			"INSERT INTO `users` (`foo`) VALUES ('bar') ON DUPLICATE KEY UPDATE `users`.`bar` = VALUES(`users`.`bar`)"},

		{users.InsertIgnore(foo).Add("bar"),
			"INSERT IGNORE INTO `users` (`foo`) VALUES ('bar')"},
		{users.InsertIgnore(foo).Add(nil),
			"INSERT IGNORE INTO `users` (`foo`) VALUES (NULL)"},
		{users.InsertIgnore(foo).Add([]byte(nil)),
			"INSERT IGNORE INTO `users` (`foo`) VALUES (NULL)"},
		{users.InsertIgnore(foo).Add([]byte{}),
			"INSERT IGNORE INTO `users` (`foo`) VALUES (X'')"},
		{users.InsertIgnore(foo).Add("bar").Add("qux"),
			"INSERT IGNORE INTO `users` (`foo`) VALUES ('bar'), ('qux')"},
		{users.InsertIgnore("foo", "bar").Add("qux", 2),
			"INSERT IGNORE INTO `users` (`foo`, `bar`) VALUES ('qux', 2)"},
		{users.InsertIgnore("foo").Add("bar").OnDupKeyUpdate("bar", 2),
			"INSERT IGNORE INTO `users` (`foo`) VALUES ('bar') ON DUPLICATE KEY UPDATE `users`.`bar` = 2"},
		{users.InsertIgnore("foo").Add("bar").OnDupKeyUpdateColumn("bar"),
			"INSERT IGNORE INTO `users` (`foo`) VALUES ('bar') ON DUPLICATE KEY UPDATE `users`.`bar` = VALUES(`users`.`bar`)"},
		{users.InsertIgnore("foo").Add("bar").OnDupKeyUpdateColumn(users.C("bar")),
			"INSERT IGNORE INTO `users` (`foo`) VALUES ('bar') ON DUPLICATE KEY UPDATE `users`.`bar` = VALUES(`users`.`bar`)"},
	}

	for _, c := range testCases {
		if sql, err := Serialize(c.builder); err != nil {
			t.Errorf("Expected success, but found %s\n%s", err, c.expected)
		} else if c.expected != sql {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expected, sql)
		}
	}
}

func TestInsertBuilderErrors(t *testing.T) {
	type User struct {
		Foo string
	}
	users := NewTable("users", User{})

	testCases := []struct {
		builder       *InsertBuilder
		expectedError string
	}{
		{users.Insert("bar").Add("bar"),
			"invalid insert column: bar"},
		{users.Insert("foo").Add("bar").OnDupKeyUpdate("bar", 2),
			"invalid update column: bar"},
		{users.Insert("foo").Add("bar").OnDupKeyUpdateColumn("bar"),
			"invalid update column: bar"},
		{users.Insert("foo").Add("bar").OnDupKeyUpdateColumn(4),
			"invalid update column: 4"},

		{users.InsertIgnore("bar").Add("bar"),
			"invalid insert column: bar"},
		{users.InsertIgnore("foo").Add("bar").OnDupKeyUpdate("bar", 2),
			"invalid update column: bar"},
		{users.InsertIgnore("foo").Add("bar").OnDupKeyUpdateColumn("bar"),
			"invalid update column: bar"},
		{users.InsertIgnore("foo").Add("bar").OnDupKeyUpdateColumn(4),
			"invalid update column: 4"},
	}

	for _, c := range testCases {
		if _, err := Serialize(c.builder); err == nil {
			t.Error("Expected error, but found success")
		} else if c.expectedError != err.Error() {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expectedError, err)
		}
	}
}

func TestReplaceBuilder(t *testing.T) {
	type User struct {
		Foo, Bar, Qux string
	}
	users := NewTable("users", User{})
	foo := users.C("foo")

	testCases := []struct {
		builder  *ReplaceBuilder
		expected string
	}{
		{users.Replace(foo).Add("bar"),
			"REPLACE INTO `users` (`foo`) VALUES ('bar')"},
		{users.Replace(foo).Add("bar").Add("qux"),
			"REPLACE INTO `users` (`foo`) VALUES ('bar'), ('qux')"},
		{users.Replace("foo", "bar").Add("qux", 2),
			"REPLACE INTO `users` (`foo`, `bar`) VALUES ('qux', 2)"},
	}

	for _, c := range testCases {
		if sql, err := Serialize(c.builder); err != nil {
			t.Errorf("Expected success, but found %s\n%s", err, c.expected)
		} else if c.expected != sql {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expected, sql)
		}
	}
}

func TestReplaceBuilderErrors(t *testing.T) {
	type User struct {
		Foo string
	}
	users := NewTable("users", User{})

	testCases := []struct {
		builder       *ReplaceBuilder
		expectedError string
	}{
		{users.Replace("bar").Add("bar"),
			"invalid replace column: bar"},
	}

	for _, c := range testCases {
		if _, err := Serialize(c.builder); err == nil {
			t.Error("Expected error, but found success")
		} else if c.expectedError != err.Error() {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expectedError, err)
		}
	}
}

func TestUpdateBuilder(t *testing.T) {
	type User struct {
		Foo, Bar, Qux string
	}
	users := NewTable("users", User{})
	foo := users.C("foo")
	bar := users.C("bar")

	testCases := []struct {
		builder  *UpdateBuilder
		expected string
	}{
		{users.Update().Set(foo, "bar"),
			"UPDATE `users` SET `users`.`foo` = 'bar'"},
		{users.Update().Set(foo, "bar").Set("bar", 4),
			"UPDATE `users` SET `users`.`foo` = 'bar', `users`.`bar` = 4"},
		{users.Update().Set(foo, "bar").Where(bar.Eq(4)),
			"UPDATE `users` SET `users`.`foo` = 'bar' WHERE `users`.`bar` = 4"},
		{users.Update().Set(foo, "bar").OrderBy(bar),
			"UPDATE `users` SET `users`.`foo` = 'bar' ORDER BY `users`.`bar`"},
		{users.Update().Set(foo, "bar").Limit(2),
			"UPDATE `users` SET `users`.`foo` = 'bar' LIMIT 2"},
	}

	for _, c := range testCases {
		if sql, err := Serialize(c.builder); err != nil {
			t.Errorf("Expected success, but found %s\n%s", err, c.expected)
		} else if c.expected != sql {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expected, sql)
		}
	}
}

func TestUpdateBuilderErrors(t *testing.T) {
	type User struct {
		Foo string
	}
	users := NewTable("users", User{})

	testCases := []struct {
		builder       *UpdateBuilder
		expectedError string
	}{
		{users.Update().Set("bar", "foo"),
			"invalid update column: bar"},
		{users.Update().Set(4, "foo"),
			"invalid update column: 4"},
	}

	for _, c := range testCases {
		if _, err := Serialize(c.builder); err == nil {
			t.Error("Expected error, but found success")
		} else if c.expectedError != err.Error() {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expectedError, err)
		}
	}
}

func TestSelectBuilder(t *testing.T) {
	type User struct {
		Foo, Bar, Qux string
	}
	users := NewTable("users", User{})
	foo := users.C("foo")
	bar := users.C("bar")
	qux := users.C("qux")

	type Object struct {
		Foo, Baz string
	}
	objects := NewTable("objects", &Object{})
	baz := objects.C("baz")

	testCases := []struct {
		builder  *SelectBuilder
		expected string
	}{
		{users.Select(1),
			"SELECT 1 FROM `users`"},
		{users.Select(L(1).Plus(1)),
			"SELECT 1+1 FROM `users`"},
		{users.Select(users.All()),
			"SELECT `users`.`bar`, `users`.`foo`, `users`.`qux` FROM `users`"},
		{users.Select("users.*"),
			"SELECT `users`.* FROM `users`"},
		{users.Select(foo.As("bar")),
			"SELECT `users`.`foo` AS `bar` FROM `users`"},
		// ComparisonExpr
		{users.Select("*").Where(foo.Eq(1)),
			"SELECT * FROM `users` WHERE `users`.`foo` = 1"},
		{users.Select("*").Where(foo.Neq(false)),
			"SELECT * FROM `users` WHERE `users`.`foo` != 0"},
		{users.Select("*").Where(foo.NullSafeEq(false)),
			"SELECT * FROM `users` WHERE `users`.`foo` <=> 0"},
		{users.Select("*").Where(foo.Gt("bar")),
			"SELECT * FROM `users` WHERE `users`.`foo` > 'bar'"},
		{users.Select("*").Where(foo.Gte(time.Time{})),
			"SELECT * FROM `users` WHERE `users`.`foo` >= '0001-01-01 00:00:00'"},
		{users.Select("*").Where(foo.Lt(2.5)),
			"SELECT * FROM `users` WHERE `users`.`foo` < 2.5"},
		{users.Select("*").Where(foo.Lt(true)),
			"SELECT * FROM `users` WHERE `users`.`foo` < 1"},
		{users.Select("*").Where(foo.IsNull()),
			"SELECT * FROM `users` WHERE `users`.`foo` IS NULL"},
		{users.Select("*").Where(foo.IsNotNull()),
			"SELECT * FROM `users` WHERE `users`.`foo` IS NOT NULL"},
		{users.Select(foo).Where(foo.In("baz", "qux")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` IN ('baz', 'qux')"},
		{users.Select(foo).Where(foo.In([]string{"baz", "qux"})),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` IN ('baz', 'qux')"},
		{users.Select(foo).Where(foo.NotIn("baz", "qux")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` NOT IN ('baz', 'qux')"},
		{users.Select(foo).Where(foo.Like("baz")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` LIKE 'baz'"},
		{users.Select(foo).Where(foo.Like("%az")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` LIKE '%az'"},
		{users.Select(foo).Where(foo.NotLike("ba_")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` NOT LIKE 'ba_'"},
		{users.Select(foo).Where(foo.RegExp("baz")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` REGEXP 'baz'"},
		{users.Select(foo).Where(foo.RegExp("*az")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` REGEXP '*az'"},
		{users.Select(foo).Where(foo.NotRegExp("ba?")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` NOT REGEXP 'ba?'"},
		// RangeCond
		{users.Select(foo).Where(foo.Between("a", "b")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` BETWEEN 'a' AND 'b'"},
		{users.Select(foo).Where(foo.NotBetween("a", "b")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` NOT BETWEEN 'a' AND 'b'"},
		// BinaryExpr
		{users.Select("*").Where(foo.Plus(1).Eq(1)),
			"SELECT * FROM `users` WHERE `users`.`foo`+1 = 1"},
		{users.Select("*").Where(foo.Plus(1).Plus(2).Eq(1)),
			"SELECT * FROM `users` WHERE (`users`.`foo`+1)+2 = 1"},
		{users.Select("*").Where(foo.Minus(1).Eq(1)),
			"SELECT * FROM `users` WHERE `users`.`foo`-1 = 1"},
		{users.Select("*").Where(foo.Mul(2).Eq(2)),
			"SELECT * FROM `users` WHERE `users`.`foo`*2 = 2"},
		{users.Select("*").Where(foo.Div(2).Eq(2)),
			"SELECT * FROM `users` WHERE `users`.`foo`/2 = 2"},
		{users.Select("*").Where(foo.Mod(2).Eq(2)),
			"SELECT * FROM `users` WHERE `users`.`foo`%2 = 2"},
		{users.Select("*").Where(foo.BitAnd(2).Eq(2)),
			"SELECT * FROM `users` WHERE `users`.`foo`&2 = 2"},
		{users.Select("*").Where(foo.BitOr(2).Eq(2)),
			"SELECT * FROM `users` WHERE `users`.`foo`|2 = 2"},
		{users.Select("*").Where(foo.BitXor(2).Eq(2)),
			"SELECT * FROM `users` WHERE `users`.`foo`^2 = 2"},
		// LogicalExpr
		{users.Select("*").Where(foo.Eq("bar").Not()),
			"SELECT * FROM `users` WHERE NOT (`users`.`foo` = 'bar')"},
		{users.Select("*").Where(foo.Eq("bar").And(bar.Lt(2))),
			"SELECT * FROM `users` WHERE (`users`.`foo` = 'bar' AND `users`.`bar` < 2)"},
		{users.Select("*").Where(foo.Eq("bar").Or(bar.Lte(2))),
			"SELECT * FROM `users` WHERE (`users`.`foo` = 'bar' OR `users`.`bar` <= 2)"},
		{users.Select("*").Where(foo.Eq("bar").And(bar.Gt(2)).Or(qux.Eq(false))),
			"SELECT * FROM `users` WHERE ((`users`.`foo` = 'bar' AND `users`.`bar` > 2) OR `users`.`qux` = 0)"},
		{users.Select("*").Where(foo.Eq("bar").And(bar.Eq("baz")).And(qux.IsNull().Or(qux.Gt(5)))),
			"SELECT * FROM `users` WHERE (`users`.`foo` = 'bar' AND `users`.`bar` = 'baz' AND (`users`.`qux` IS NULL OR `users`.`qux` > 5))"},
		// GroupBy
		{users.Select(foo).Where(foo.Eq("bar")).GroupBy(bar),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` = 'bar' GROUP BY `users`.`bar`"},
		{users.Select(foo).Where(foo.Eq("bar")).GroupBy(foo, bar),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` = 'bar' GROUP BY `users`.`foo`, `users`.`bar`"},
		{users.Select(foo).Where(foo.Eq("bar")).GroupBy(bar).OrderBy(qux.Ascending()),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` = 'bar' GROUP BY `users`.`bar` ORDER BY `users`.`qux` ASC"},
		// OrderBy
		{users.Select(foo).Where(foo.Eq("bar")).OrderBy(foo, qux),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` = 'bar' ORDER BY `users`.`foo`, `users`.`qux`"},
		{users.Select(foo).Where(foo.Eq("bar")).OrderBy(qux.Descending()),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` = 'bar' ORDER BY `users`.`qux` DESC"},
		{users.Select(foo).Where(foo.Eq("bar")).OrderBy(foo.Descending(), qux),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` = 'bar' ORDER BY `users`.`foo` DESC, `users`.`qux`"},
		// Limit and Offset
		{users.Select(foo).Where(foo.Eq("bar")).Limit(1),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` = 'bar' LIMIT 1"},
		{users.Select(foo).Where(foo.Eq("bar")).Limit(1).Offset(10),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` = 'bar' LIMIT 10, 1"},
		{users.Select(foo, qux),
			"SELECT `users`.`foo`, `users`.`qux` FROM `users`"},
		// Grouping
		{users.Select(foo).Where(users.C("foo", "bar").Eq(G("baz", "qux"))),
			"SELECT `users`.`foo` FROM `users` WHERE (`users`.`foo`, `users`.`bar`) = ('baz', 'qux')"},
		// Having
		{users.Select(foo).Having(foo.Eq("bar")),
			"SELECT `users`.`foo` FROM `users` HAVING `users`.`foo` = 'bar'"},
		// Functions
		{users.Select(foo.Count()),
			"SELECT COUNT(`users`.`foo`) FROM `users`"},
		{users.Select(foo.CountDistinct()),
			"SELECT COUNT(DISTINCT `users`.`foo`) FROM `users`"},
		{users.Select(foo.Max()),
			"SELECT MAX(`users`.`foo`) FROM `users`"},
		{users.Select(foo.Min()),
			"SELECT MIN(`users`.`foo`) FROM `users`"},
		{users.Select(foo.Func("FOO")),
			"SELECT FOO(`users`.`foo`) FROM `users`"},
		{users.Select(foo.FuncDistinct("COUNT")),
			"SELECT COUNT(DISTINCT `users`.`foo`) FROM `users`"},
		{users.Select(users.C("foo", "bar").Count()),
			"SELECT COUNT(`users`.`foo`, `users`.`bar`) FROM `users`"},
		{users.Select(users.C("foo", "bar").CountDistinct()),
			"SELECT COUNT(DISTINCT `users`.`foo`, `users`.`bar`) FROM `users`"},
		{users.Select(foo.Func("SUM").As("total")),
			"SELECT SUM(`users`.`foo`) AS `total` FROM `users`"},
		// Miscellaneous
		{users.Select("*").Distinct(),
			"SELECT DISTINCT * FROM `users`"},
		{users.Select("*").ForUpdate(),
			"SELECT * FROM `users` FOR UPDATE"},
		{users.Select("*").WithSharedLock(),
			"SELECT * FROM `users` LOCK IN SHARE MODE"},
		// Joins
		{users.InnerJoin(objects).Select("*"),
			"SELECT * FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).Select(users.All()),
			"SELECT `users`.`bar`, `users`.`foo`, `users`.`qux` FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).Select(foo),
			"SELECT `users`.`foo` FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).Select(baz),
			"SELECT `objects`.`baz` FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).Select(foo, baz),
			"SELECT `users`.`foo`, `objects`.`baz` FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).Select("objects.foo"),
			"SELECT `objects`.`foo` FROM `users` INNER JOIN `objects`"},
		{users.InnerJoin(objects).On(foo.Eq(baz)).Select("*"),
			"SELECT * FROM `users` INNER JOIN `objects` ON `users`.`foo` = `objects`.`baz`"},
		{users.InnerJoin(objects).On(foo.Eq(objects.C("foo"))).Select("*"),
			"SELECT * FROM `users` INNER JOIN `objects` ON `users`.`foo` = `objects`.`foo`"},
		{users.InnerJoin(objects).Using(foo).Select("*"),
			"SELECT * FROM `users` INNER JOIN `objects` USING (`foo`)"},
		{users.InnerJoin(objects).Using("foo").Select("*"),
			"SELECT * FROM `users` INNER JOIN `objects` USING (`foo`)"},
		{users.LeftJoin(objects).Select("*"),
			"SELECT * FROM `users` LEFT JOIN `objects`"},
		{users.RightJoin(objects).Select("*"),
			"SELECT * FROM `users` RIGHT JOIN `objects`"},
		// Subquery
		{users.Select("*").Where(foo.InTuple(&Subquery{objects.Select(objects.C("foo")).Where(objects.C("foo").Gt(10))})),
			"SELECT * FROM `users` WHERE `users`.`foo` IN (SELECT `objects`.`foo` FROM `objects` WHERE `objects`.`foo` > 10)"},
	}

	for _, c := range testCases {
		if sql, err := Serialize(c.builder); err != nil {
			t.Errorf("Expected success, but found %s\n%s", err, c.expected)
		} else if c.expected != sql {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expected, sql)
		}
	}
}

func TestSerializeWithPlaceholders(t *testing.T) {
	type User struct {
		Foo, Bar, Qux string
	}
	users := NewTable("users", User{})
	foo := users.C("foo")

	testCases := []struct {
		builder  *SelectBuilder
		expected string
	}{
		{users.Select(foo.As("bar")),
			"SELECT `users`.`foo` AS `bar` FROM `users`"},
		{users.Select("*").Where(foo.Eq(1)),
			"SELECT * FROM `users` WHERE `users`.`foo` = ?"},
		{users.Select("*").Where(foo.Neq(false)),
			"SELECT * FROM `users` WHERE `users`.`foo` != ?"},
		{users.Select("*").Where(foo.NullSafeEq(false)),
			"SELECT * FROM `users` WHERE `users`.`foo` <=> ?"},
		{users.Select("*").Where(foo.Gte(time.Time{})),
			"SELECT * FROM `users` WHERE `users`.`foo` >= ?"},
		{users.Select("*").Where(foo.Lt(2.5)),
			"SELECT * FROM `users` WHERE `users`.`foo` < ?"},
		{users.Select("*").Where(foo.Lt(true)),
			"SELECT * FROM `users` WHERE `users`.`foo` < ?"},
		{users.Select("*").Where(foo.IsNull()),
			"SELECT * FROM `users` WHERE `users`.`foo` IS NULL"},
		{users.Select(foo).Where(foo.In([]string{"baz", "qux"})),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` IN (?, ?)"},
		{users.Select(foo).Where(foo.Like("baz")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` LIKE ?"},
		{users.Select(foo).Where(foo.RegExp("*a*")),
			"SELECT `users`.`foo` FROM `users` WHERE `users`.`foo` REGEXP ?"},
		{users.UseIndex("index_one").Select("*").Where(foo.Eq(1)),
			"SELECT * FROM `users` USE INDEX (index_one) WHERE `users`.`foo` = ?"},
		{users.ForceIndex("index_one", "index_two").Select("*").Where(foo.Eq(1)),
			"SELECT * FROM `users` FORCE INDEX (index_one, index_two) WHERE `users`.`foo` = ?"},
		{users.IgnoreIndex("index_one", "index_two", "index_three").Select("*").Where(foo.Eq(1)),
			"SELECT * FROM `users` IGNORE INDEX (index_one, index_two, index_three) WHERE `users`.`foo` = ?"},
	}

	for _, c := range testCases {
		if sql, err := SerializeWithPlaceholders(c.builder); err != nil {
			t.Errorf("Expected success, but found %s\n%s", err, c.expected)
		} else if c.expected != sql {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expected, sql)
		}
		if users.IndexHints != nil {
			t.Errorf("IndexHints should not be set on original table")
		}
	}
}

func TestSelectBuilderErrors(t *testing.T) {
	type User struct {
		Foo string
	}
	users := NewTable("users", User{})

	type Object struct {
		Foo string
	}
	objects := NewTable("objects", &Object{})

	testCases := []struct {
		builder       *SelectBuilder
		expectedError string
	}{
		{users.Select("foobar"),
			"unknown column: foobar",
		},
		{users.Select("foo.*"),
			"unknown table: foo",
		},
		{users.Select("foo.foo"),
			"unknown table: foo",
		},
		{users.Select(struct{}{}),
			"unsupported type struct {}, a struct",
		},
		{users.Select(users.C("foo").As(";")),
			"invalid AS identifier: ;",
		},
		{users.Select(users.C("foo").Func(";")),
			"invalid FUNC identifier: ;",
		},
		{users.Select("*").Where(G().Eq("foo")),
			"empty group",
		},
		{users.Select("*").Where(users.C("foo").In()),
			"empty list"},
		{users.Select("*").Where(users.C("foo").NotIn()),
			"empty list"},
		{users.InnerJoin(objects).Using("bar").Select("*"),
			"invalid join column: bar",
		},
		{users.InnerJoin(objects).Select("foo"),
			"ambiguous column: foo",
		},
	}

	for _, c := range testCases {
		if _, err := Serialize(c.builder); err == nil {
			t.Error("Expected error, but found success")
		} else if c.expectedError != err.Error() {
			t.Errorf("Expected\n%s\nbut got\n%s", c.expectedError, err)
		}
	}
}

func ExampleTable_Select() {
	type User struct {
		ID string
	}
	users := NewTable("users", User{})
	id := users.C("id")
	q := users.Select("*").Where(id.Eq("bar"))
	if sql, err := Serialize(q); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(sql)
	}
	// Output: SELECT * FROM `users` WHERE `users`.`id` = 'bar'
}

func ExampleTable_Join() {
	type User struct {
		ID string
	}
	users := NewTable("users", User{})
	type Object struct {
		ID string
	}
	objects := NewTable("objects", Object{})
	q := users.InnerJoin(objects).Using("id").Select("*")
	if sql, err := Serialize(q); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(sql)
	}
	// Output: SELECT * FROM `users` INNER JOIN `objects` USING (`id`)
}

func TestJoinBuilder(t *testing.T) {
	type user struct {
		ID        int    `db:"id"`
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	type address struct {
		UserID  int    `db:"user_id"`
		ZipCode int    `db:"zip_code"`
		Street  string `db:"street"`
	}
	type vehicle struct {
		UserID int    `db:"user_id"`
		Make   string `db:"make"`
		Model  string `db:"model"`
	}

	testCases := []struct {
		description, expectedSQL string
		user, vehicle, address   *Table
	}{
		{
			description: "All aliased",
			user:        NewAliasedTable("user", "u", user{}),
			vehicle:     NewAliasedTable("vehicle", "v", vehicle{}),
			address:     NewAliasedTable("address", "a", address{}),
			expectedSQL: "SELECT `v`.`model`, `u`.`first_name`, `a`.`zip_code` FROM `user` AS `u` " +
				"INNER JOIN `address` AS `a` ON `u`.`id` = `a`.`user_id` " +
				"INNER JOIN `vehicle` AS `v` ON `u`.`id` = `v`.`user_id` " +
				"WHERE `u`.`id` = 1234",
		},
		{
			description: "No aliases",
			user:        NewTable("user", user{}),
			vehicle:     NewTable("vehicle", vehicle{}),
			address:     NewTable("address", address{}),
			expectedSQL: "SELECT `vehicle`.`model`, `user`.`first_name`, `address`.`zip_code` FROM `user` " +
				"INNER JOIN `address` ON `user`.`id` = `address`.`user_id` " +
				"INNER JOIN `vehicle` ON `user`.`id` = `vehicle`.`user_id` " +
				"WHERE `user`.`id` = 1234",
		},
		{
			description: "Some aliases",
			user:        NewTable("user", user{}),
			vehicle:     NewAliasedTable("vehicle", "v", vehicle{}),
			address:     NewTable("address", address{}),
			expectedSQL: "SELECT `v`.`model`, `user`.`first_name`, `address`.`zip_code` FROM `user` " +
				"INNER JOIN `address` ON `user`.`id` = `address`.`user_id` " +
				"INNER JOIN `vehicle` AS `v` ON `user`.`id` = `v`.`user_id` " +
				"WHERE `user`.`id` = 1234",
		},
	}

	for _, testCase := range testCases {
		q := testCase.user.InnerJoin(testCase.address).On(testCase.user.C("id").Eq(testCase.address.C("user_id"))).
			InnerJoin(testCase.vehicle).On(testCase.user.C("id").Eq(testCase.vehicle.C("user_id"))).
			Select(testCase.vehicle.C("model"), testCase.user.C("first_name"), testCase.address.C("zip_code")).
			Where(testCase.user.C("id").Eq(1234))

		if sql, err := Serialize(q); err != nil {
			t.Fatal(err)
		} else if sql != testCase.expectedSQL {
			t.Errorf("Expected\n %s\n but have\n %s\n", testCase.expectedSQL, sql)
		}
	}
}
