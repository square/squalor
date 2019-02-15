# Squalor [![Circle CI](https://circleci.com/gh/square/squalor/tree/master.png?style=badge)](https://circleci.com/gh/square/squalor/tree/master)

Squalor is a library of SQL utilities for marshalling and
unmarshalling of model structs into table rows and programmatic
construction of SQL statements. It is a combination of the
functionality in ORM-like libraries such as
[gorp](https://github.com/coopernurse/gorp), SQL utility libraries
such as [sqlx](https://github.com/jmoiron/sqlx) and SQL construction
libraries such as
[sqlbuilder](https://github.com/dropbox/godropbox/tree/master/database/sqlbuilder). Squalor helps ensure your programs don't contains SQL injection (SQLi) bugs.

## Sample code
```go
package main

import (
  "database/sql"
  "fmt"

  _ "github.com/go-sql-driver/mysql"
  "github.com/square/squalor"
)

type Book struct {
  ID   int  `db:"id"`
  Title string `db:"title"`
  Author int `db:"author"`
}

func main()  {
  _db, err := sql.Open("mysql", "root@/test_db")
  panicOnError(err)

  // Create a test database
  _, err = _db.Exec("DROP TABLE IF EXISTS books")
  panicOnError(err)
  _, err = _db.Exec("CREATE TABLE books (id int primary key, title varchar(255), author int)")
  panicOnError(err)

  // Bind the Go struct with the database
  db := squalor.NewDB(_db)
  book := &Book{}
  books, err := db.BindModel("books", book)
  panicOnError(err)

  // Sample inserts
  book = &Book{ID: 1, Title: "Defender Of Greatness", Author: 1234}
  err = db.Insert(book)
  panicOnError(err)

  book = &Book{ID: 2, Title: "Destiny Of Silver", Author: 1234}
  err = db.Insert(book)
  panicOnError(err)

  // Sample query by primary key
  err = db.Get(book, 2)
  panicOnError(err)
  fmt.Printf("%v\n", book)

  // More complicated query
  q := books.Select(books.All()).Where(books.C("author").Eq(1234))
  var results []Book
  err = db.Select(&results, q)
  panicOnError(err)
  fmt.Printf("results: %v\n", results)
}

func panicOnError(err error) {
  if err != nil {
    panic(err)
  }
}
```

## API Documentation

Full godoc output from the latest code in master is available here:

http://godoc.org/github.com/square/squalor

## Limitations

- While squalor uses the database/sql package, the SQL it utilizes is
MySQL specific (e.g. REPLACE, INSERT ON DUPLICATE KEY UPDATE, etc).
- squalor cannot handle non-UTF-8 strings.

## History

Squalor started as an experiment to provide programmatic construction
of SQL statements to protected against SQL injection attacks that can
sneak into code when using printf-style construction. Such SQL
injection attacks can occur even in the presence of placeholders if
the programmer accidentally uses user data either without a
placeholder or in a portion of the SQL statement where a placeholder
cannot be used (e.g. for a column name).

The programmatic SQL construction experiment then combined with an
experiment to optimize batch operations in gorp. The internal changes
to gorp were significant enough to necessitate a fork to get this
done. Some of the API was adjusted via learnings from sqlx (e.g. the
removal of TypeConverter).

Squalor emerged from these experiments to satisfy internal short term
needs. It is being released in the hopes that others will learn from
it and find it useful just as we've learned from and utilized gorp,
sqlx and sqlbuilder.

## LICENSE

    Copyright 2014 Square, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
