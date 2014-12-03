# Squalor #

Squalor is a library of SQL utilities for marshalling and
unmarshalling of model structs into table rows and programmatic
construction of SQL statements. It is a combination of the
functionality in ORM-like libraries such as
[gorp](https://github.com/coopernurse/gorp), SQL utility libraries
such as [sqlx](https://github.com/jmoiron/sqlx) and SQL construction
libraries such as
[sqlbuilder](https://github.com/dropbox/godropbox/tree/master/database/sqlbuilder).

## API Documentation ##

Full godoc output from the latest code in master is available here:

http://godoc.org/github.com/square/squalor

## Limitations ##

While squalor uses the database/sql package, the SQL it utilizes is
MySQL specific (e.g. REPLACE, INSERT ON DUPLICATE KEY UPDATE, etc).

## History ##

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
