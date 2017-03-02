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

/*
Package squalor provides SQL utility routines, such as validation of
models against table schemas, marshalling and unmarshalling of model
structs into table rows and programmatic construction of SQL
statements.

Limitations

While squalor uses the database/sql package, the SQL it utilizes is
MySQL specific (e.g. REPLACE, INSERT ON DUPLICATE KEY UPDATE, etc).

Model Binding and Validation

Given a simple table definition:

    CREATE TABLE users (
      id   bigint       PRIMARY KEY NOT NULL,
      name varchar(767) NOT NULL,
      INDEX (id, name)
    )

And a model for a row of the table:

    type User struct {
      ID   int64  `db:"id"`
      Name string `db:"name"`
    }

The BindModel method will validate that the model and the table
definition are compatible. For example, it would be an error for the
User.ID field to have the type string.

    users, err := db.BindModel("users", User{})

The table definition is loaded from the database allowing custom
checks such as verifying the existence of indexes.

    if users.GetKey("id") == nil {
        // The index ("id") does not exist!
    }
    if users.GetKey("id", "name") == nil {
        // The index ("id", "name") does not exist!
    }

Custom Types

While fields are often primitive types such as int64 or string, it is
sometimes desirable to use a custom type. This can be accomplished by
having the type implement the sql.Scanner and driver.Valuer
interfaces. For example, you might create a GzippedText type which
automatically compresses the value when it is written to the database
and uncompressed when it is read:

    type GzippedText []byte
    func (g GzippedText) Value() (driver.Value, error) {
        buf := &bytes.Buffer{}
        w := gzip.NewWriter(buf)
        defer w.Close()
        w.Write(g)
        return buf.Bytes(), nil
    }
    func (g *GzippedText) Scan(src interface{}) error {
        var source []byte
        switch t := src.(type) {
        case string:
            source = []byte(t)
        case []byte:
            source = t
        default:
            return errors.New("Incompatible type for GzippedText")
        }
        reader, err := gzip.NewReader(bytes.NewReader(source))
        defer reader.Close()
        b, err := ioutil.ReadAll(reader)
        if err != nil {
            return err
        }
        *g = GzippedText(b)
        return nil
    }

Insert

The Insert method is used to insert one or more rows in a table.

    err := db.Insert(User{ID:42, Name:"Peter Mattis"})

You can pass either a struct or a pointer to a struct.

    err := db.Insert(&User{ID:43, Name:"Spencer Kimball"})

Multiple rows can be inserted at once. Doing so offers convenience and
performance.

    err := db.Insert(&User{ID:42, Name:"Peter Mattis"},
        &User{ID:43, Name:"Spencer Kimball"})

When multiple rows are batch inserted the returned error corresponds
to the first SQL error encountered which may make it impossible to
determine which row caused the error. If the rows correspond to
different tables the order of insertion into the tables is
undefined. If you care about the order of insertion into multiple
tables and determining which row is causing an insertion error,
structure your calls to Insert appropriately (i.e. insert into a
single table or insert a single row).

After a successful insert on a table with an auto increment primary
key, the auto increment will be set back in the corresponding field of
the object. For example, if the user table was defined as:

    CREATE TABLE users (
      id   bigint       PRIMARY KEY AUTO INCREMENT NOT NULL,
      name varchar(767) NOT NULL,
      INDEX (id, name)
    )

Then we could create a new user by doing:

    u := &User{Name:"Peter Mattis"}
    err := db.Insert(u)
    if err != nil {
        ...
    }
    // u.ID will be correctly populated at this point

Replace

The Replace method replaces a row in table, either inserting the row
if it doesn't exist, or deleting and then inserting the row. See the
MySQL docs for the difference between REPLACE, UPDATE and INSERT ON
DUPLICATE KEY UPDATE (Upsert).

    err := db.Replace(&User{ID:42, Name:"Peter Mattis"})

Update

The Update method updates a row in a table, returning the number of
rows modified.

    count, err := db.Update(&User{ID:42, Name:"Peter Mattis"})

Upsert

The Upsert method inserts or updates a row.

    err := db.Upsert(&User{ID:42, Name:"Peter Mattis"})

Get

The Get method retrieves a single row by primary key and binds the
result columns to a struct.

    user := &User{}
    err := db.Get(user, 42)
    // Returns an error if user 42 cannot be found.

Delete

The delete method deletes rows by primary key.

   err := db.Delete(&User{ID:42})

See the documentation for DB.Delete for performance limitations when
batch deleting multiple rows from a table with a primary key composed
of multiple columns.

Optimistic Locking

To support optimistic locking with a column storing the version number,
one field in a model object can be marked to serve as the lock. Modifying
the example above:

    type User struct {
      ID   int64  `db:"id"`
      Name string `db:"name"`
      Ver  int    `db:"version,optlock"`
    }

Now, the Update method will ensure that the object has not been concurrently
modified when writing, by constraining the update by the version number.
If the update is successful, the version number will be both incremented
on the model (in-memory), as well as in the database.

Programmatic SQL Construction

Programmatic construction of SQL queries prohibits SQL injection
attacks while keeping query construction both readable and similar to
SQL itself. Support is provided for most of the SQL DML (data
manipulation language), though the constructed queries are targetted
to MySQL.

    q := users.Select("*").Where(users.C("id").Eq(foo))
    // squalor.Serialize(q) == "SELECT `users`.* FROM users WHERE `users`.`id` = 'foo'", nil
    var results []User
    err := db.Select(&results, q)

DB provides wrappers for the sql.Query and sql.QueryRow
interfaces. The Rows struct returned from Query has an additional
StructScan method for binding the columns for a row result to a
struct.

    rows, err := db.Query(q)
    if err != nil {
      return err
    }
    defer rows.Close()
    for rows.Next() {
      u := User{}
      if err := rows.StructScan(&u); err != nil {
        return err
      }
      // Process u
    }

QueryRow can be used to easily query and scan a single value:

    var count int
    err := db.QueryRow(users.Select(users.C("id").Count())).Scan(&count)

Performance

In addition to the convenience of inserting, deleting and updating
table rows using a struct, attention has been paid to
performance. Since squalor is carefully constructing the SQL queries
for insertion, deletion and update, it eschews the use of placeholders
in favor of properly escaping values in the query. With the Go MySQL
drivers, this saves a roundtrip to the database for each query because
queries with placeholders must be prepared before being executed.

Marshalling and unmarshalling of data from structs utilizes
reflection, but care is taken to minimize the use of reflection in
order to improve performance. For example, reflection data for models
is cached when the model is bound to a table.

Batch operations are utilized when possible. The Delete, Insert,
Replace, Update and Upsert operations will perform multiple operations
per SQL statement. This can provide an order of magnitude speed
improvement over performing the mutations one row at a time due to
minimizing the network overhead.
*/
package squalor
