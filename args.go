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
	"context"
	"fmt"
	"reflect"
)

var baseTypes = map[reflect.Type]bool{
	reflect.TypeOf(bool(true)): true,
	reflect.TypeOf(int(0)):     true,
	reflect.TypeOf(int8(0)):    true,
	reflect.TypeOf(int16(0)):   true,
	reflect.TypeOf(int32(0)):   true,
	reflect.TypeOf(int64(0)):   true,
	reflect.TypeOf(uint(0)):    true,
	reflect.TypeOf(uint8(0)):   true,
	reflect.TypeOf(uint16(0)):  true,
	reflect.TypeOf(uint32(0)):  true,
	reflect.TypeOf(uint64(0)):  true,
	reflect.TypeOf(float32(0)): true,
	reflect.TypeOf(float64(0)): true,
	reflect.TypeOf(string("")): true,
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

var kindsToBaseType = map[reflect.Kind]reflect.Type{
	reflect.Bool:    reflect.TypeOf(bool(true)),
	reflect.Int:     reflect.TypeOf(int(0)),
	reflect.Int8:    reflect.TypeOf(int8(0)),
	reflect.Int16:   reflect.TypeOf(int16(0)),
	reflect.Int32:   reflect.TypeOf(int32(0)),
	reflect.Int64:   reflect.TypeOf(int64(0)),
	reflect.Uint:    reflect.TypeOf(uint(0)),
	reflect.Uint8:   reflect.TypeOf(uint8(0)),
	reflect.Uint16:  reflect.TypeOf(uint16(0)),
	reflect.Uint32:  reflect.TypeOf(uint32(0)),
	reflect.Uint64:  reflect.TypeOf(uint64(0)),
	reflect.Float32: reflect.TypeOf(float32(0)),
	reflect.Float64: reflect.TypeOf(float64(0)),
	reflect.String:  reflect.TypeOf(string("")),
}

type contextKey string

func (c contextKey) String() string {
	return "square/squalor context key " + string(c)
}

var (
	ContextKeyComments = contextKey("comments")
)

func argsConvert(from []interface{}) []interface{} {
	to := make([]interface{}, len(from))
	for i, arg := range from {
		value := reflect.ValueOf(arg)
		if baseTypes[value.Type()] {
			// Base type, let it through unchanged.
			to[i] = arg
		} else if baseKinds[value.Kind()] {
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

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint()

	case reflect.Float32, reflect.Float64:
		return value.Float()

	case reflect.String:
		return value.String()

	default:
		panic(fmt.Sprintf("unmapped base kind %s", kind))
	}
}

func comments(ctx context.Context) ([]string, bool) {
	comment, ok := ctx.Value(ContextKeyComments).([]string)
	return comment, ok
}
