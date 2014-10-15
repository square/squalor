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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

func sqlBaseType(sqlType string) string {
	if index := strings.IndexAny(sqlType, "( "); index != -1 {
		return sqlType[:index]
	}
	return sqlType
}

var (
	byteSliceType = reflect.TypeOf([]byte(""))
	setStringType = reflect.TypeOf(map[string]struct{}{})
	scannerType   = reflect.TypeOf(new(sql.Scanner)).Elem()
	valuerType    = reflect.TypeOf(new(driver.Valuer)).Elem()
	timeType      = reflect.TypeOf(time.Time{})
)

func validateIntType(bits int, sqlType string, goType reflect.Type) bool {
	if strings.HasSuffix(sqlType, " unsigned") {
		switch goType.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return bits == goType.Bits()
		}
	} else {
		switch goType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return bits == goType.Bits()
		}
	}
	return false
}

func validateBitType(sqlType string, goType reflect.Type) bool {
	bits := 0
	if n, err := fmt.Sscanf(sqlType, "bit(%d)", &bits); err != nil {
		return false
	} else if n != 1 {
		return false
	}
	requiredBits := []int{8, 16, 32, 64}
	i := sort.SearchInts(requiredBits, bits)
	if i >= len(requiredBits) {
		return false
	}
	bits = requiredBits[i]
	switch goType.Kind() {
	case reflect.Int, reflect.Uint:
		fallthrough
	case reflect.Int8, reflect.Uint8:
		fallthrough
	case reflect.Int16, reflect.Uint16:
		fallthrough
	case reflect.Int32, reflect.Uint32:
		fallthrough
	case reflect.Int64, reflect.Uint64:
		return bits == goType.Bits()
	}
	return false
}

func validateFloatType(bits int, goType reflect.Type) bool {
	switch goType.Kind() {
	case reflect.Float32:
		return bits == 32
	case reflect.Float64:
		return bits == 64
	}
	return false
}

func validateDateTimeType(goType reflect.Type) bool {
	switch {
	case goType.Kind() == reflect.Int64:
		return true
	case goType == timeType:
		return true
	}
	return false
}

func validateStringType(goType reflect.Type) bool {
	switch {
	case goType.Kind() == reflect.String:
		return true
	case goType == byteSliceType:
		return true
	}
	return false
}

func validateModelType(sqlType string, goType reflect.Type) error {
	// If the go type implements the driver.Valuer and sql.Scanner
	// interfaces, just ignore the sql type and assume it is doing the
	// right thing. It would be possible to instantiate the go type and
	// check to see if it can handle the sql type, but that seems overly
	// complicated for the benefit.
	if goType.Implements(valuerType) &&
		reflect.PtrTo(goType).Implements(scannerType) {
		return nil
	}

	sqlType = strings.ToLower(sqlType)
	baseType := sqlBaseType(sqlType)

	valid := false
	switch baseType {
	case "int":
		valid = validateIntType(32, sqlType, goType)
	case "tinyint":
		// The "(1)" is really a formatting specifier, but MySQL uses it
		// when "bool" or "boolean" is specified as the type.
		valid = (sqlType == "tinyint(1)" && goType.Kind() == reflect.Bool)
		valid = valid || validateIntType(8, sqlType, goType)
	case "smallint":
		valid = validateIntType(16, sqlType, goType)
	case "mediumint":
		valid = validateIntType(32, sqlType, goType)
	case "bigint":
		valid = validateIntType(64, sqlType, goType)
	case "bit":
		valid = validateBitType(sqlType, goType)
	case "float":
		valid = validateFloatType(32, goType)
	case "double":
		valid = validateFloatType(64, goType)
	case "decimal":
		valid = validateFloatType(64, goType)
	case "date", "time", "datetime", "timestamp":
		valid = validateDateTimeType(goType)
	case "char", "varchar", "binary", "varbinary":
		fallthrough
	case "blob", "tinyblob", "mediumblob", "longblob":
		fallthrough
	case "text", "tinytext", "mediumtext", "longtext":
		valid = validateStringType(goType)
	case "enum":
		valid = (goType.Kind() == reflect.String)
	case "set":
		valid = (goType == setStringType)
	}

	if valid {
		return nil
	}
	return fmt.Errorf("incompatible types: \"%s\" vs \"%s\"", sqlType, goType)
}
