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
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	// Need to force vendor/mysql to be imported in order for the
	// "mysql" sql driver to be registered.
	_ "github.com/go-sql-driver/mysql"
)

func makeTestDSN(dbName string) string {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "root")
	if password := os.Getenv("MYSQL_PASSWORD"); password != "" {
		fmt.Fprintf(&buf, ":%s", password)
	}
	fmt.Fprint(&buf, "@")
	host := os.Getenv("MYSQL_HOST")

	if host == "" {
		host = "localhost"
	}
	if !strings.Contains(host, ":") {
		host += ":3306"
	}
	fmt.Fprintf(&buf, "tcp(%s)/%s", host, dbName)

	return buf.String()
}

func makeTestDB(t testing.TB, ddls ...string) *DB {
	return makeTestDBWithOptions(t, []DBOption{}, ddls...)
}

func makeTestDBWithOptions(t testing.TB, options []DBOption, ddls ...string) *DB {
	const dbName = "squalor_test"

	// We need to create an initial database connection for the purpose
	// of creating our testing database.
	tmpDB, err := sql.Open("mysql", makeTestDSN(""))
	if err != nil {
		t.Fatal(err)
	}
	defer tmpDB.Close()
	tmpDB.SetMaxOpenConns(1)

	if _, err := tmpDB.Exec("SET sql_notes=0"); err != nil {
		t.Fatal(err)
	}
	if _, err := tmpDB.Exec("DROP DATABASE IF EXISTS " + dbName); err != nil {
		t.Fatal(err)
	}
	if _, err := tmpDB.Exec("CREATE DATABASE IF NOT EXISTS " + dbName); err != nil {
		t.Fatal(err)
	}

	// Now create our actual database connection.
	db, err := sql.Open("mysql", makeTestDSN(dbName))
	if err != nil {
		t.Fatal(err)
	}
	for _, ddl := range ddls {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	newDB, _ := NewDB(db, options...)
	return newDB
}
