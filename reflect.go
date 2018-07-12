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
	"reflect"
	"strings"
)

type fieldDesc struct {
	reflect.StructField
	optlock bool
}

type fieldMap map[string]fieldDesc

const (
	tagName    = "db"
	tagOptlock = "optlock"
)

// getMapping returns a mapping for the type t, using the tagName and
// the mapFunc to determine the canonical names of fields. Based on
// reflectx.getMapping, but the returned map is from string to
// reflect.StructField instead of string to index slice.
func getMapping(t reflect.Type, mapFunc func(string) string) fieldMap {
	type typeQueue struct {
		t reflect.Type
		p []int
	}

	queue := []typeQueue{typeQueue{deref(t), []int{}}}
	m := fieldMap{}
	for len(queue) != 0 {
		// Pop the first item off of the queue.
		tq := queue[0]
		queue = queue[1:]
		// Iterate through all of its fields.
		for fieldPos := 0; fieldPos < tq.t.NumField(); fieldPos++ {
			f := tq.t.Field(fieldPos)

			name, optlock := readTag(f.Tag.Get(tagName))

			// Breadth first search of untagged anonymous embedded structs.
			if f.Anonymous && f.Type.Kind() == reflect.Struct && name == "" {
				queue = append(queue, typeQueue{deref(f.Type), appendIndex(tq.p, fieldPos)})
				continue
			}

			// Skip unexported fields.
			if len(f.PkgPath) != 0 {
				continue
			}

			// If the name is "-", disabled via a tag, skip it.
			if name == "-" {
				continue
			}

			if len(name) == 0 {
				if mapFunc != nil {
					name = mapFunc(f.Name)
				} else {
					name = f.Name
				}
			}

			// If the name is shadowed by an earlier identical name in the
			// search, skip it.
			if _, ok := m[name]; ok {
				continue
			}
			// Add it to the map at the current position.
			sf := f
			sf.Index = appendIndex(tq.p, fieldPos)
			m[name] = fieldDesc{sf, optlock}
		}
	}
	return m
}

func readTag(tag string) (string, bool) {
	if comma := strings.Index(tag, ","); comma != -1 {
		return tag[0:comma], tag[comma+1:len(tag)] == tagOptlock
	} else {
		return tag, false
	}
}

func getDBFields(t reflect.Type) fieldMap {
	return getMapping(t, strings.ToLower)
}

// getTraversals returns the field traversals (for use by
// reflect.{Value,Type}.FieldByIndex) for the named fields.
func (m fieldMap) getTraversals(names []string) [][]int {
	var traversals [][]int
	for _, name := range names {
		f, ok := m[name]
		if !ok {
			panic(fmt.Errorf("db field '%s' has no mapping", name))
		}
		traversals = append(traversals, f.Index)
	}
	return traversals
}

func (m fieldMap) getMappedColumns(columns []*Column, ignoreUnmappedCols, ignoreMissingCols bool) ([]*Column, error) {
	mapped := make(map[string]bool)
	var mappedColumns []*Column
	for _, col := range columns {
		_, ok := m[col.Name]
		if !ok {
			if !ignoreUnmappedCols {
				return nil, fmt.Errorf("db field '%s' has no mapping", col.Name)
			}
			continue
		}
		mappedColumns = append(mappedColumns, col)
		mapped[col.Name] = true
	}
	if !ignoreMissingCols && len(mapped) != len(m) {
		var notMapped []string
		for name := range m {
			if !mapped[name] {
				notMapped = append(notMapped, name)
			}
		}
		return nil, fmt.Errorf("model fields '%s' have no corresponding db field", strings.Join(notMapped, ", "))
	}
	return mappedColumns, nil
}

func (m fieldMap) getOptlockColumnNameAndInc() (*string, func(reflect.Value), error) {
	var (
		optlockColumnName string
		optlockInc        func(reflect.Value)
		found             bool
	)
	for name, f := range m {
		if f.optlock {
			if found {
				return nil, nil, fmt.Errorf("model has two columns marked for optimistic locking")
			}
			var (
				k     = f.Type.Kind()
				index = f.Index
			)
			if k == reflect.Int || k == reflect.Int8 ||
				k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64 {
				optlockInc = func(v reflect.Value) {
					ver := v.FieldByIndex(index)
					ver.SetInt(ver.Int() + 1)
				}
			} else if k == reflect.Uint || k == reflect.Uint8 || k == reflect.Uint16 ||
				k == reflect.Uint32 || k == reflect.Uint64 {
				optlockInc = func(v reflect.Value) {
					ver := v.FieldByIndex(index)
					ver.SetUint(ver.Uint() + 1)
				}
			} else {
				return nil, nil, fmt.Errorf("model field '%s' must be of int or uint kind to be marked for optimistic locking", f.Name)
			}
			optlockColumnName = name
			found = true
		}
	}
	if found {
		return &optlockColumnName, optlockInc, nil
	}
	return nil, nil, nil
}

// deref is Indirect for reflect.Type
func deref(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

// A copying append that creates a new slice each time.
func appendIndex(is []int, i int) []int {
	// Make a new slice with capacity for a single additional element.
	x := make([]int, 0, len(is)+1)
	// Append the old slice to the new slice and then append the new
	// element.
	return append(append(x, is...), i)
}
