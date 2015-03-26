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
	"reflect"
	"strings"
)

type fieldMap map[string]reflect.StructField

// getMapping returns a mapping for the type t, using the tagName and
// the mapFunc to determine the canonical names of fields. Based on
// reflectx.getMapping, but the returned map is from string to
// reflect.StructField instead of string to index slice.
func getMapping(t reflect.Type, tagName string, mapFunc func(string) string) fieldMap {
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

			// Skip unexported fields.
			if len(f.PkgPath) != 0 {
				continue
			}

			name := f.Tag.Get(tagName)

			// If the name is "-", disabled via a tag, skip it.
			if name == "-" {
				continue
			}

			// Breadth first search of untagged anonymous embedded structs.
			if f.Anonymous && f.Type.Kind() == reflect.Struct && name == "" {
				queue = append(queue, typeQueue{deref(f.Type), appendIndex(tq.p, fieldPos)})
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
			m[name] = sf
		}
	}
	return m
}

func getDBFields(t reflect.Type) fieldMap {
	return getMapping(t, "db", strings.ToLower)
}

// getTraversals returns the field traversals (for use by
// reflect.{Value,Type}.FieldByIndex) for the named fields.
func (m fieldMap) getTraversals(names []string) [][]int {
	var traversals [][]int
	for _, name := range names {
		if f, ok := m[name]; ok {
			traversals = append(traversals, f.Index)
		}
	}
	return traversals
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
