// Copyright 2015 Square Inc.
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
	"reflect"
)

var baseTypes = map[reflect.Type]bool{
	reflect.TypeOf(new(bool)):    true,
	reflect.TypeOf(new(int)):     true,
	reflect.TypeOf(new(int8)):    true,
	reflect.TypeOf(new(int16)):   true,
	reflect.TypeOf(new(int32)):   true,
	reflect.TypeOf(new(int64)):   true,
	reflect.TypeOf(new(uint)):    true,
	reflect.TypeOf(new(uint8)):   true,
	reflect.TypeOf(new(uint16)):  true,
	reflect.TypeOf(new(uint32)):  true,
	reflect.TypeOf(new(uint64)):  true,
	reflect.TypeOf(new(float32)): true,
	reflect.TypeOf(new(float64)): true,
	reflect.TypeOf(new(string)):  true,
}

var baseKinds = map[reflect.Kind]bool{
	reflect.Bool:    true,
	reflect.Int:     true,
	reflect.Int8:    true,
	reflect.Int16:   true,
	reflect.Int32:   true,
	reflect.Int64:   true,
	reflect.Uint:    true,
	reflect.Uint8:   true,
	reflect.Uint16:  true,
	reflect.Uint32:  true,
	reflect.Uint64:  true,
	reflect.Float32: true,
	reflect.Float64: true,
	reflect.String:  true,
}

func argsConvert(from []interface{}) []interface{} {
	to := make([]interface{}, len(from))
	for i, arg := range from {
		value := reflect.ValueOf(arg)
		if _, ok := baseTypes[value.Type()]; ok {
			// Base type, let it through unchanged.
			to[i] = arg
		} else if _, ok := baseKinds[value.Kind()]; ok {
			// Type alias, convert to base type.
			to[i] = asKind(value)
		} else {
			// Other, deferring to lower-level libraries, and will likely result in a
			// conversion error at the database/sql layer.
			to[i] = arg
		}
	}
	return to
}

func asKind(value reflect.Value) interface{} {
	kind := value.Kind()
	switch kind {
	case reflect.Bool:
		return value.Bool()

	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		return value.Int()

	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		return value.Uint()

	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		return value.Float()

	case reflect.String:
		return value.String()

	default:
		panic(fmt.Sprintf("unmapped base kind %s", kind))
	}
}
