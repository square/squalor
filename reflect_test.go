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
	"testing"
)

func fieldByName(m fieldMap, v reflect.Value, name string) reflect.Value {
	v = reflect.Indirect(v)
	f, ok := m[name]
	if !ok {
		return reflect.Value{}
	}
	return v.FieldByIndex(f.Index)
}

func TestBasic(t *testing.T) {
	type Foo struct {
		A, B, C int64
	}

	f := Foo{1, 2, 3}
	fv := reflect.ValueOf(f)
	m := getMapping(reflect.TypeOf(f), nil)

	v := fieldByName(m, fv, "A")
	if v.Int() != f.A {
		t.Errorf("Expected %d, got got %d", f.A, v.Int())
	}
	v = fieldByName(m, fv, "B")
	if v.Int() != f.B {
		t.Errorf("Expected %d, got got %d", f.B, v.Int())
	}
	v = fieldByName(m, fv, "C")
	if v.Int() != f.C {
		t.Errorf("Expected %d, got got %d", f.C, v.Int())
	}
}

func TestEmbedded(t *testing.T) {
	type Foo struct {
		A int64
	}

	type Bar struct {
		Foo
		B int64
	}

	type Baz struct {
		A int64
		Bar
	}

	z := Baz{}
	z.A = 1
	z.B = 2
	z.Bar.Foo.A = 3
	zv := reflect.ValueOf(z)
	m := getMapping(reflect.TypeOf(z), nil)

	v := fieldByName(m, zv, "A")
	if v.Int() != z.A {
		t.Errorf("Expecting %d, got %d", z.A, v.Int())
	}
	v = fieldByName(m, zv, "B")
	if v.Int() != z.B {
		t.Errorf("Expecting %d, got %d", z.B, v.Int())
	}
}

func TestPrivate(t *testing.T) {
	type foo struct {
		A int64
	}

	type Bar struct {
		foo
		B int64
	}

	z := Bar{}
	z.A = 1
	z.B = 2
	zv := reflect.ValueOf(z)

	m := getMapping(reflect.TypeOf(z), nil)

	v := fieldByName(m, zv, "A")
	if v.Int() != z.A {
		t.Errorf("Expecting %d, got %d", z.A, v.Int())
	}
	v = fieldByName(m, zv, "B")
	if v.Int() != z.B {
		t.Errorf("Expecting %d, got %d", z.B, v.Int())
	}
}

func TestMapping(t *testing.T) {
	type Foo struct {
		A int
		B string
		C bool `db:"foo"`
	}
	f := Foo{1, "two", true}
	m := getDBFields(reflect.TypeOf(f))
	for _, key := range []string{"a", "b", "foo"} {
		if _, ok := m[key]; !ok {
			t.Errorf("Expected to find key %s in mapping but did not", key)
		}
	}

	type Bar struct {
		D int
		E int
		Foo
	}
	b := Bar{D: 3, E: 4, Foo: f}
	m = getDBFields(reflect.TypeOf(b))
	for _, key := range []string{"a", "b", "foo", "d", "e"} {
		if _, ok := m[key]; !ok {
			t.Errorf("Expected to find key %s in mapping but did not", key)
		}
	}

	type Qux struct {
		Bar     `db:"baz"`
		F       int
		G       bool `db:"qux"`
		Ignored bool `db:"-"`
	}
	q := Qux{b, 5, true, false}
	m = getDBFields(reflect.TypeOf(q))
	for _, key := range []string{"baz", "f", "qux"} {
		if _, ok := m[key]; !ok {
			t.Errorf("Expected to find key %s in mapping but did not", key)
		}
	}

	if _, ok := m["ignored"]; ok {
		t.Errorf("Expected to ignore `Ignored` field")
	}
}

func TestReadTag(t *testing.T) {
	testCases := []struct {
		tag     string
		name    string
		optlock bool
	}{
		{"-", "-", false},
		{"foo", "foo", false},
		{"foo,", "foo", false},
		{"foo,optlock", "foo", true},
		{",optlock", "", true},
		{",wrong", "", false},
		{",", "", false},
	}
	for _, c := range testCases {
		name, optlock := readTag(c.tag)
		if name != c.name {
			t.Errorf("%s: expected name '%s', actual '%s'", c.tag, c.name, name)
		}
		if optlock != c.optlock {
			t.Errorf("%s: expected optlock '%t', actual '%t'", c.tag, c.optlock, optlock)
		}
	}
}

type benchmarkType struct {
	A, B, C, D, E, F, G, H, I int
	J, K, L, M, N, O, P, Q, R int
	S, T, U, V, W, X, Y, Z    int
}

func BenchmarkGorpUnmarshal(b *testing.B) {
	t := reflect.TypeOf(benchmarkType{})
	m := getDBFields(t)

	var traversals [][]int
	for _, f := range m {
		traversals = append(traversals, f.Index)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This loop is intended to mimic the core of gorp
		// unmarshalling. In particular, gorp creates a new "values" slice
		// for each iteration of the loop even though this could be pulled
		// outside. And the "vp.Elem()" call happens within the inner loop
		// instead of being pulled out like sqlx-style unmarshalling.
		vp := reflect.New(t)
		values := make([]interface{}, len(traversals))

		for j := range traversals {
			v := vp.Elem()
			values[j] = v.FieldByIndex(traversals[j]).Addr().Interface()
		}

		for _, q := range values {
			*q.(*int) = i
		}
	}
}

// fieldByIndex returns a value for a particular struct traversal.
func fieldByIndex(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
		// if this is a pointer, it's possible it is nil
		if v.Kind() == reflect.Ptr && v.IsNil() {
			v.Set(reflect.New(deref(v.Type())))
		}
	}
	return v
}

func BenchmarkSqlxUnmarshal(b *testing.B) {
	t := reflect.TypeOf(benchmarkType{})
	m := getDBFields(t)

	var traversals [][]int
	for _, f := range m {
		traversals = append(traversals, f.Index)
	}

	values := make([]interface{}, len(traversals))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This loop is intended to mimic the core of sqlx
		// unmarshalling. For each row scanned it creates a new result
		// object and sets up the values array to contain pointers to the
		// fields of the object.
		vp := reflect.New(t)
		v := vp.Elem()

		for j, traversal := range traversals {
			values[j] = fieldByIndex(v, traversal).Addr().Interface()
		}

		for _, q := range values {
			*q.(*int) = i
		}
	}
}

func BenchmarkDBUnmarshal(b *testing.B) {
	t := reflect.TypeOf(benchmarkType{})
	m := getDBFields(t)

	zero := reflect.Zero(t)
	vp := reflect.New(t)
	v := vp.Elem()

	values := make([]interface{}, 0, len(m))
	for _, f := range m {
		values = append(values, v.FieldByIndex(f.Index).Addr().Interface())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This loop is intended to mimic the core of our unmarshalling
		// approach. Outside the loop we set up our "values" slice which
		// contains pointers to the fields in a prototype object. Inside
		// the loop we simply scan and clone the prototype object.
		for _, q := range values {
			*q.(*int) = i
		}
		v.Set(zero)
	}
}
