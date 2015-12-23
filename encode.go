// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file
//
// SQUARE NOTE: The encoding routines were derived from vitess's
// sqltypes package. The original source can be found at
// https://code.google.com/p/vitess/

package squalor

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"
)

var (
	dontEscape  = byte(255)
	nullstr     = []byte("NULL")
	singleQuote = []byte("'")
	backslash   = []byte("\\")
	hexStart    = []byte("X'")
	// encodeMap specifies how to escape binary data with '\'.
	// Complies to http://dev.mysql.com/doc/refman/5.7/en/string-literals.html
	encodeMap [256]byte
	// decodeMap is the reverse of encodeMap
	decodeMap [256]byte
	hexMap    [256][]byte
)

// Retrieve a tmp buffer for use during SQL value encoding. The
// contents of the buffer will only survive until the next call to
// io.Writer.Write.
func getTmpBuffer(w io.Writer, n int) []byte {
	if buf, ok := w.(*bytes.Buffer); ok {
		buf.Grow(n)
		b := buf.Bytes()
		return b[len(b):]
	}
	return nil
}

func encodeSQLValue(w io.Writer, arg interface{}) error {
	// Use sql.driver to convert the arg to a sql.Value which is simply
	// an interface{} with a restricted set of types. This also takes
	// care of using the sql.Valuer interface to convert arbitrary types
	// into sql.Values.
	dv, err := driver.DefaultParameterConverter.ConvertValue(arg)
	if err != nil {
		// We may be in the presence of a type alias not supported by the
		// database/driver DefaultParameterConverter. Special handling.
		value := reflect.ValueOf(arg)
		if baseKinds[value.Kind()] {
			return encodeSQLValue(w, asKind(value))
		}
		return err
	}
	switch v := dv.(type) {
	case nil:
		_, err := w.Write(nullstr)
		return err
	case bool:
		var b []byte
		if v {
			b = astBoolTrue
		} else {
			b = astBoolFalse
		}
		_, err := w.Write(b)
		return err
	case int64:
		tmp := getTmpBuffer(w, 64)
		_, err := w.Write(strconv.AppendInt(tmp, v, 10))
		return err
	case float64:
		tmp := getTmpBuffer(w, 64)
		_, err := w.Write(strconv.AppendFloat(tmp, v, 'f', -1, 64))
		return err
	case string:
		return encodeSQLString(w, v)
	case []byte:
		// A nil []byte still has the type []byte and ends up here, not in
		// the "case nil" above.
		if v == nil {
			_, err := w.Write(nullstr)
			return err
		}
		return encodeSQLBytes(w, v)
	case time.Time:
		_, err := io.WriteString(w, v.Format("'2006-01-02 15:04:05.999999'"))
		return err
	}
	return fmt.Errorf("unsupported type %T: %v", arg, arg)
}

func encodeSQLString(w io.Writer, in string) error {
	if _, err := w.Write(singleQuote); err != nil {
		return err
	}
	start := 0
	for i := 0; i < len(in); i++ {
		ch := in[i]
		if encodedChar := encodeMap[ch]; encodedChar != dontEscape {
			if start != i {
				if _, err := io.WriteString(w, in[start:i]); err != nil {
					return err
				}
			}
			start = i + 1
			if _, err := w.Write(backslash); err != nil {
				return err
			}
			if _, err := w.Write([]byte{encodedChar}); err != nil {
				return err
			}
		}
	}
	if start < len(in) {
		if _, err := io.WriteString(w, in[start:]); err != nil {
			return err
		}
	}
	_, err := w.Write(singleQuote)
	return err
}

func encodeSQLBytes(w io.Writer, v []byte) error {
	if _, err := w.Write(hexStart); err != nil {
		return err
	}
	for _, d := range v {
		if _, err := w.Write(hexMap[d]); err != nil {
			return err
		}
	}
	_, err := w.Write(singleQuote)
	return err
}

func init() {
	encodeRef := map[byte]byte{
		'\x00': '0',
		'\'':   '\'',
		'"':    '"',
		'\b':   'b',
		'\n':   'n',
		'\r':   'r',
		'\t':   't',
		26:     'Z', // ctl-Z
		'\\':   '\\',
	}

	for i := range encodeMap {
		encodeMap[i] = dontEscape
		decodeMap[i] = dontEscape
	}
	for i := range encodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			encodeMap[byte(i)] = to
			decodeMap[to] = byte(i)
		}
	}
	for i := range hexMap {
		hexMap[i] = []byte(fmt.Sprintf("%02x", i))
	}
}
