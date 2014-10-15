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
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

func TestSqlBaseType(t *testing.T) {
	testCases := []struct {
		sqlType  string
		baseType string
	}{
		{"int(11)", "int"},
		{"tinyint(4)", "tinyint"},
		{"smallint(6)", "smallint"},
		{"mediumint(9)", "mediumint"},
		{"bigint(20)", "bigint"},
		{"int(10) unsigned", "int"},
		{"tinyint(3) unsigned", "tinyint"},
		{"smallint(5) unsigned", "smallint"},
		{"mediumint(8) unsigned", "mediumint"},
		{"bigint(20) unsigned", "bigint"},
		{"bit(8)", "bit"},
		{"float", "float"},
		{"double", "double"},
		{"decimal(10,0)", "decimal"},
		{"date", "date"},
		{"time", "time"},
		{"datetime", "datetime"},
		{"timestamp", "timestamp"},
		{"char(255)", "char"},
		{"varchar(255)", "varchar"},
		{"binary(255)", "binary"},
		{"varbinary(255)", "varbinary"},
		{"blob", "blob"},
		{"tinyblob", "tinyblob"},
		{"mediumblob", "mediumblob"},
		{"longblob", "longblob"},
		{"text", "text"},
		{"tinytext", "tinytext"},
		{"mediumtext", "mediumtext"},
		{"longtext", "longtext"},
		{"enum('a','b','c')", "enum"},
		{"set('a','b','c')", "set"},
	}
	for _, c := range testCases {
		baseType := sqlBaseType(c.sqlType)
		if c.baseType != baseType {
			t.Errorf("Expected %s, but got %s", c.baseType, baseType)
		}
	}
}

type testValuer struct{}

var _ driver.Valuer = testValuer{}

func (t testValuer) Value() (driver.Value, error) {
	return 0, nil
}

type testScanner struct{}

var _ sql.Scanner = &testScanner{}

func (t *testScanner) Scan(src interface{}) error {
	return nil
}

type testValuerAndScanner struct{}

var _ driver.Valuer = testValuerAndScanner{}
var _ sql.Scanner = &testValuerAndScanner{}

func (t testValuerAndScanner) Value() (driver.Value, error) {
	return 0, nil
}

func (t *testValuerAndScanner) Scan(src interface{}) error {
	return nil
}

func TestValidateModelType(t *testing.T) {
	testCases := []struct {
		sqlType         string
		goType          interface{}
		expectedSuccess bool
	}{
		{"int", int(0), false},
		{"int", int8(0), false},
		{"int", int16(0), false},
		{"int", int32(0), true},
		{"int", int64(0), false},
		{"int", uint(0), false},
		{"int", uint8(0), false},
		{"int", uint16(0), false},
		{"int", uint32(0), false},
		{"int", uint64(0), false},

		{"tinyint(1)", false, true},
		{"tinyint(1)", int8(0), true},
		{"tinyint(1)", int16(0), false},
		{"tinyint(2)", false, false},

		{"tinyint", int(0), false},
		{"tinyint", int8(0), true},
		{"tinyint", int16(0), false},
		{"tinyint", int32(0), false},
		{"tinyint", int64(0), false},
		{"tinyint", uint(0), false},
		{"tinyint", uint8(0), false},
		{"tinyint", uint16(0), false},
		{"tinyint", uint32(0), false},
		{"tinyint", uint64(0), false},

		{"smallint", int(0), false},
		{"smallint", int8(0), false},
		{"smallint", int16(0), true},
		{"smallint", int32(0), false},
		{"smallint", int64(0), false},
		{"smallint", uint(0), false},
		{"smallint", uint8(0), false},
		{"smallint", uint16(0), false},
		{"smallint", uint32(0), false},
		{"smallint", uint64(0), false},

		{"mediumint", int(0), false},
		{"mediumint", int8(0), false},
		{"mediumint", int16(0), false},
		{"mediumint", int32(0), true},
		{"mediumint", int64(0), false},
		{"mediumint", uint(0), false},
		{"mediumint", uint8(0), false},
		{"mediumint", uint16(0), false},
		{"mediumint", uint32(0), false},
		{"mediumint", uint64(0), false},

		{"bigint", int(0), true},
		{"bigint", int8(0), false},
		{"bigint", int16(0), false},
		{"bigint", int32(0), false},
		{"bigint", int64(0), true},
		{"bigint", uint(0), false},
		{"bigint", uint8(0), false},
		{"bigint", uint16(0), false},
		{"bigint", uint32(0), false},
		{"bigint", uint64(0), false},

		{"int unsigned", int(0), false},
		{"int unsigned", int8(0), false},
		{"int unsigned", int16(0), false},
		{"int unsigned", int32(0), false},
		{"int unsigned", int64(0), false},
		{"int unsigned", uint(0), false},
		{"int unsigned", uint8(0), false},
		{"int unsigned", uint16(0), false},
		{"int unsigned", uint32(0), true},
		{"int unsigned", uint64(0), false},

		{"tinyint unsigned", int(0), false},
		{"tinyint unsigned", int8(0), false},
		{"tinyint unsigned", int16(0), false},
		{"tinyint unsigned", int32(0), false},
		{"tinyint unsigned", int64(0), false},
		{"tinyint unsigned", uint(0), false},
		{"tinyint unsigned", uint8(0), true},
		{"tinyint unsigned", uint16(0), false},
		{"tinyint unsigned", uint32(0), false},
		{"tinyint unsigned", uint64(0), false},

		{"smallint unsigned", int(0), false},
		{"smallint unsigned", int8(0), false},
		{"smallint unsigned", int16(0), false},
		{"smallint unsigned", int32(0), false},
		{"smallint unsigned", int64(0), false},
		{"smallint unsigned", uint(0), false},
		{"smallint unsigned", uint8(0), false},
		{"smallint unsigned", uint16(0), true},
		{"smallint unsigned", uint32(0), false},
		{"smallint unsigned", uint64(0), false},

		{"mediumint unsigned", int(0), false},
		{"mediumint unsigned", int8(0), false},
		{"mediumint unsigned", int16(0), false},
		{"mediumint unsigned", int32(0), false},
		{"mediumint unsigned", int64(0), false},
		{"mediumint unsigned", uint(0), false},
		{"mediumint unsigned", uint8(0), false},
		{"mediumint unsigned", uint16(0), false},
		{"mediumint unsigned", uint32(0), true},
		{"mediumint unsigned", uint64(0), false},

		{"bigint unsigned", int(0), false},
		{"bigint unsigned", int8(0), false},
		{"bigint unsigned", int16(0), false},
		{"bigint unsigned", int32(0), false},
		{"bigint unsigned", int64(0), false},
		{"bigint unsigned", uint(0), true},
		{"bigint unsigned", uint8(0), false},
		{"bigint unsigned", uint16(0), false},
		{"bigint unsigned", uint32(0), false},
		{"bigint unsigned", uint64(0), true},

		{"bit(1)", int8(0), true},
		{"bit(1)", uint8(0), true},
		{"bit(1)", int16(0), false},
		{"bit(8)", int8(0), true},
		{"bit(8)", int16(0), false},
		{"bit(9)", int8(0), false},
		{"bit(9)", int16(0), true},
		{"bit(16)", int16(0), true},
		{"bit(16)", int32(0), false},
		{"bit(17)", int16(0), false},
		{"bit(17)", int32(0), true},
		{"bit(32)", int32(0), true},
		{"bit(32)", int64(0), false},
		{"bit(33)", int64(0), true},
		{"bit(64)", int64(0), true},

		{"float", float32(0), true},
		{"float", float64(0), false},
		{"double", float32(0), false},
		{"double", float64(0), true},
		{"decimal", float64(0), true},

		{"date", int32(0), false},
		{"date", int64(0), true},
		{"date", time.Now(), true},
		{"time", int32(0), false},
		{"time", int64(0), true},
		{"time", time.Now(), true},
		{"datetime", int32(0), false},
		{"datetime", int64(0), true},
		{"datetime", time.Now(), true},
		{"timestamp", int32(0), false},
		{"timestamp", int64(0), true},
		{"timestamp", time.Now(), true},

		{"char(255)", string(""), true},
		{"char(255)", []byte(""), true},
		{"varchar(255)", string(""), true},
		{"varchar(255)", []byte(""), true},
		{"binary(255)", string(""), true},
		{"binary(255)", []byte(""), true},
		{"varbinary(255)", string(""), true},
		{"varbinary(255)", []byte(""), true},
		{"blob", string(""), true},
		{"blob", []byte(""), true},
		{"tinyblob", string(""), true},
		{"tinyblob", []byte(""), true},
		{"mediumblob", string(""), true},
		{"mediumblob", []byte(""), true},
		{"longblob", string(""), true},
		{"longblob", []byte(""), true},
		{"text", string(""), true},
		{"text", []byte(""), true},
		{"tinytext", string(""), true},
		{"tinytext", []byte(""), true},
		{"mediumtext", string(""), true},
		{"mediumtext", []byte(""), true},
		{"longtext", string(""), true},
		{"longtext", []byte(""), true},

		{"text", struct{}{}, false},
		{"text", testValuer{}, false},
		{"text", testScanner{}, false},
		{"text", testValuerAndScanner{}, true},

		{"enum('a','b','c')", string(""), true},
		{"enum('a','b','c')", 0, false},

		{"set('a','b','c')", map[string]struct{}{}, true},
		{"set('a','b','c')", string(""), false},
	}
	for _, c := range testCases {
		err := validateModelType(c.sqlType, reflect.TypeOf(c.goType))
		if c.expectedSuccess && err != nil {
			t.Errorf("Expected success, but got %s", err)
		} else if !c.expectedSuccess && err == nil {
			t.Errorf("Expected failure, but found success: %s %T", c.sqlType, c.goType)
		}
	}
}

func TestValidateModelCustomType(t *testing.T) {
	err := validateModelType("text", reflect.TypeOf(testValuerAndScanner{}))
	if err != nil {
		t.Error(err)
	}
}
