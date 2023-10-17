// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package squalor

import (
	"bytes"
	"math"
	"testing"
	"time"
)

func TestEncodeSQL(t *testing.T) {
	oneShift63Uint := uint(1) << 63     //lowest uint with high bit set
	oneShift63Uint64 := uint64(1) << 63 //lowest uint64 with high bit set
	binaryOneZeroRepeating := (uint64(1)<<(2*31)-1)/3<<3 | 2

	type boolAlias bool
	type int32Alias int32
	type intAlias int
	type int64Alias int64
	type uintAlias uint
	type uint64Alias uint64
	type stringAlias string

	oneShift63UintAlias := uintAlias(oneShift63Uint)
	oneShift63Uint64Alias := uint64Alias(oneShift63Uint64)

	testCases := []struct {
		arg      interface{}
		expected string
	}{
		{nil, "NULL"},
		{(*intAlias)(nil), "NULL"},
		{(*int32)(nil), "NULL"},
		{(*int32Alias)(nil), "NULL"},
		{(*stringAlias)(nil), "NULL"},
		{(*string)(nil), "NULL"},
		{(*uint)(nil), "NULL"},
		{(*uint64)(nil), "NULL"},
		{(*uintAlias)(nil), "NULL"},
		{(*uint64Alias)(nil), "NULL"},
		{true, "1"},
		{false, "0"},
		{boolAlias(true), "1"},
		{boolAlias(false), "0"},
		{int(-1), "-1"},
		{intAlias(-1), "-1"},
		{int32(-1), "-1"},
		{int32Alias(-1), "-1"},
		{int64(-1), "-1"},
		{int64(math.MinInt64), "-9223372036854775808"},
		{int64(math.MaxInt64), "9223372036854775807"},
		{int64Alias(math.MaxInt64), "9223372036854775807"},
		{uint(1), "1"},
		{uint32(1), "1"},
		{uint64(1), "1"},
		{oneShift63Uint64 - 1, "9223372036854775807"},
		{oneShift63Uint, "9223372036854775808"},
		{oneShift63UintAlias, "9223372036854775808"},
		{oneShift63Uint64, "9223372036854775808"},
		{oneShift63Uint64Alias, "9223372036854775808"},
		{&oneShift63Uint, "9223372036854775808"},
		{&oneShift63UintAlias, "9223372036854775808"},
		{&oneShift63Uint64, "9223372036854775808"},
		{&oneShift63Uint64Alias, "9223372036854775808"},
		{binaryOneZeroRepeating, "12297829382473034410"},
		{uint64(math.MaxUint64), "18446744073709551615"},
		{1.23, "1.23"},
		{math.SmallestNonzeroFloat32, "0.000000000000000000000000000000000000000000001401298464324817"},
		{-math.SmallestNonzeroFloat32, "-0.000000000000000000000000000000000000000000001401298464324817"},
		{math.SmallestNonzeroFloat64, "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005"},
		{-math.SmallestNonzeroFloat64, "-0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005"},
		{math.MaxFloat32, "340282346638528860000000000000000000000"},
		{-math.MaxFloat32, "-340282346638528860000000000000000000000"},
		{math.MaxFloat64, "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"},
		{-math.MaxFloat64, "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"},
		{"abcd", "'abcd'"},
		{stringAlias("abcd"), "'abcd'"},
		{"workin' hard", "'workin\\' hard'"},
		{"\x00'\"\b\n\r\t\x1A\\", `'\0\'\"\b\n\r\t\Z\\'`},
		{[]byte(nil), "NULL"},
		{[]byte{}, "X''"},
		{[]byte("abcd"), "X'61626364'"},
		{[]byte("\x00'\"\b\n\r\t\x1A\\"), "X'002722080a0d091a5c'"},
		{time.Date(2012, time.February, 24, 23, 19, 43, 10, time.UTC), "'2012-02-24 23:19:43'"},
		{time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC), "'1999-01-02 03:04:05'"},
		{time.Date(2015, 3, 4, 5, 6, 7, 987654000, time.UTC), "'2015-03-04 05:06:07.987654'"},
		// Three different representations of the same unicode string.
		{"\xE7\xB1\xB3\xE6\xB4\xBE", "'米派'"},
		{"\u7C73\u6D3E", "'米派'"},
		{"米派", "'米派'"},
	}
	for _, c := range testCases {
		var buf bytes.Buffer
		if err := encodeSQLValue(&buf, c.arg); err != nil {
			t.Error(err)
			continue
		}
		encoded := buf.String()
		if encoded != c.expected {
			t.Errorf("Expected %q, but got %q", c.expected, encoded)
		}
	}
}

// Ensure dontEscape is not escaped
func TestDontEscape(t *testing.T) {
	if encodeMap[dontEscape] != dontEscape {
		t.Errorf("Encode fail: %v", encodeMap[dontEscape])
	}
	if decodeMap[dontEscape] != dontEscape {
		t.Errorf("Decode fail: %v", decodeMap[dontEscape])
	}
}

func TestEncodeSQLComment(t *testing.T) {
	testCases := []struct {
		arg      string
		expected string
	}{
		{"", "/*  */"},
		{"/*/", "/* /*\\/ */"},
		{"foo", "/* foo */"},
		{"/* foo */", "/* foo */"},
		{"/*foo*/", "/*foo*/"},
		{"foo */", "/* foo *\\/ */"},
		{"/* foo */ */", "/* foo *\\/ */"},
		{"-- foo", "/* -- foo */"},
		{"# foo", "/* # foo */"},
		{"米派", "/* 米派 */"},
	}
	for _, c := range testCases {
		var buf bytes.Buffer
		if err := encodeSQLComment(&buf, c.arg); err != nil {
			t.Error(err)
			continue
		}
		encoded := buf.String()
		if encoded != c.expected {
			t.Errorf("Expected %q, but got %q", c.expected, encoded)
		}
	}
}

func TestEscapeCommentContents(t *testing.T) {
	testCases := []struct {
		arg      string
		expected string
	}{
		{"", ""},
		{"/*/", "/*\\/"},
		{"foo", "foo"},
		{"/* foo */", "/* foo *\\/"},
		{"/*foo*/", "/*foo*\\/"},
		{"*/ */ */", "*\\/ *\\/ *\\/"},
		{"米派", "米派"},
	}
	for _, c := range testCases {
		var buf bytes.Buffer
		if err := escapeCommentContents(&buf, c.arg); err != nil {
			t.Error(err)
			continue
		}
		encoded := buf.String()
		if encoded != c.expected {
			t.Errorf("Expected %q, but got %q", c.expected, encoded)
		}
	}
}
