![Zō](media/logo.svg) 

# Zō Clojure PostgreSQL Client

Zō is a Clojure-first Postgres client that provides an idiomatic Clojure
experience to leverage PostgreSQL-specifc features.

The goal for Zō is great everyday use out of the box, while also making
it simple to use custom data type conversions.

## Release and Dependency Information

Zō is under active development and is alpha in at least two senses: it
hasn't been extensively tested and the API isn't stable.

[`tools.deps`](https://github.com/clojure/tools.deps.alpha) dependency
information:

```clojure
com.grzm/zo.alpha {:mvn/version "0.1.1"}
```

[Leiningen][lein]/[Boot][boot] dependency information:

[boot]: https://github.com/boot-clj/boot
[lein]: https://github.com/technomancy/leiningen

```clojure
[com.grzm/zo.alpha "0.1.1"]
```

## Why should I use Zō?

- Define custom, low-level data type conversions
- Transform data directly into the shape used by your application
- Receive `LISTEN/NOTIFY` notifications using [core.async][].

[core.async]: https://github.com/clojure/core.async

The [clojure.java.jdbc][java.jdbc] wrapper is a great way to use
Postgres. Its `ISQLParameter` and `IResultSetReadColumn` protocols
make it easier to use data types not specified by JDBC. You can go a
long way being productive with Postgres using `clojure.java.jdbc`.

[java.jdbc]: https://github.com/clojure/java.jdbc

### So why use Zō?

If you're interested in using custom data types and 

#### Low-level custom data type conversions

A JDBC driver tranforms data to and from standard `java.sql`
types. Any custom types or Postgres extensions like `ARRAY[]` are
wrapped in generic objects. In any event, you're likely going to do
further tranforms of the data into the language of your
application. Zō allows you to define these tranformations directly
without requiring the intermediate step through the standard JDBC
types.

#### Realize Results Once

A JDBC driver decodes the data rows sent by the database backend and
creates a ResultSet which it hands over to the application. The
application often iterates over the ResultSet to tranform it into
app-specific data.

Zō passes over the data once, without realizing an intermediate
ResultSet.  Any custom decoders specified by the user likewise operate
directly on the rows as they're returned by the backend.

#### LISTEN/NOTIFY with core.async

Zō provides access to PostgreSQL [`LISTEN/NOTIFY`][listen/notify]
notifications via core.async channels, making it easy to process
notifications as they arrive.

[listen/notify]: https://www.postgresql.org/docs/10/static/sql-notify.html

## Usage

### Queries

```clojure
(require '[net.zopg.zo :as zo])

(def client (zo/client {:}))

(def sess (zo/connect client))

;; simple query
(zo/q sess {:sql "SELECT FROM "})
;; => 

;; parameterized query

(zo/q sess {:sql "SELECT $1::INT4, $2::BOOL" [42 true]})
;; => 
```

## Custom types and codecs

## Zō as a driver toolkit

The components which make up the default Zō implementation are defined
in terms of protocols. You can create your own implementations to use
different type resolution or statement caching strategies.


## Testing

Use `boot test` or `boot watch alt-test` to run the unit tests.

Some tests require a running PostgreSQL instance. The tests use the
[Pique][pique] library to determine the connection: use normal libpq
[environment variables][libpq-envars], [password files][libpq-pgpass],
and [service files][libpq-pgservice] to set the configuration. If no
host is provided, `localhost` is assumed.

[pique]: https://github.com/grzm/pique.alpha
[libpq-envars]: https://www.postgresql.org/docs/10/static/libpq-envars.html
[libpq-pgpass]: https://www.postgresql.org/docs/10/static/libpq-pgpass.html
[libpq-pgservice]: https://www.postgresql.org/docs/10/static/libpq-pgservice.html

    export PGPORT=5455 PGDATABASE=zo_test PGUSER=zo_tester
    boot test

### Testing in CIDER

To run tests in [CIDER[cider], use the Emacs `setenv` function to set
environment variables such as `PGHOST` or `PGSYSCONFDIR` *prior to
calling `cider-jack-in`*. For example:

```elisp
(setenv "PGSYSCONFDIR" "/Users/myuser/pg")
(setenv "PGPASSFILE" "/Users/myuser/pg/.pgpass")
(setenv "PGSERVICE" "mydb")
```
Note these are *elisp* functions, not Clojure.

Another alternative is to use [environ][]. Copy
`test/resources/.boot-env-template` to `.boot-env` and update it with
your local configuration.

    cp test/resources/.boot-env-template test/resources/.boot-env

[environ]: https://github.com/weavejester/environ
[cider]: https://github.com/clojure-emacs/cider

### Benchmarks

To run the benchmarks, from the command line run

    script/bench

## License

© 2018 Michael Glaesemann

Released under the MIT License.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
