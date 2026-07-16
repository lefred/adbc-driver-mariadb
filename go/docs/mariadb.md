---
# Copyright (c) 2026 lefred - MariaDB Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
{}
---

{{ cross_reference|safe }}
# MariaDB Driver {{ version }}

{{ heading|safe }}

This driver provides access to [MariaDB][mariadb], the free and open-source
relational database management system.

## Installation

The MariaDB driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install mariadb
```

## Connecting

To connect, edit the `uri` option below to match your environment and run the following:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
  driver="mariadb",
  db_kwargs = {
    "uri": "mariadb://root@localhost:3306/demo"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers. See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

### Connection String Format

Connection strings are passed with the `uri` option. The driver supports two formats:

#### MariaDB URI Format (Recommended)

The driver accepts a MariaDB-specific URI:

```text
mariadb://[user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...]
```

Examples:

- `mariadb://localhost/mydb`
- `mariadb://user:pass@localhost:3306/mydb`
- `mariadb://user:pass@host/db?charset=utf8mb4&timeout=30s`
- `mariadb://user@(/path/to/socket.sock)/db` (Unix domain socket)
- `mariadb://user@localhost/mydb` (no password)

URI Components:
- `scheme`: `mariadb://` (required)
- `user`: Optional (for authentication)
- `password`: Optional (for authentication, requires user)
- `host`: Required (must be explicitly specified)
- `port`: Optional (defaults to 3306)
- `schema`: Optional (can be empty, MariaDB database name)
- Query params: MariaDB connection attributes

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`. If you include a zone ID in an IPv6 address, the `%` character used as the separator must be replaced with `%25`.
:::

Unix Domain Sockets:
When connecting via Unix domain sockets, use the parentheses syntax to wrap the socket path: `mariadb://user@(/path/to/socket.sock)/db`

The `mysql://` scheme is intentionally rejected. Native Go DSNs remain supported
for compatibility with the underlying wire-protocol library.

#### Go MariaDB Driver DSN Format (Alternative)

The driver also accepts the [Go MariaDB Driver DSN format](https://github.com/go-sql-driver/mysql?tab=readme-ov-file#dsn-data-source-name):

```text
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
```

Examples:

- `user:pass@tcp(localhost:3306)/mydb`
- `user@tcp(127.0.0.1:3306)/mydb`
- `user:pass@unix(/tmp/mariadb.sock)/mydb`

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

In addition to the shared SQL types, metadata-based schema discovery supports:

- `UUID` as the standard Arrow `arrow.uuid` extension type.
- `VECTOR(n)` as `fixed_size_list<float32>[n]`. If a query result omits the
  vector width, the driver conservatively returns packed binary data with
  `mariadb.vector_encoding=float32_le` metadata.
- `INET4` and `INET6` as canonical strings carrying the intended logical type
  in `mariadb.logical_arrow_type`. The driver deliberately avoids unregistered
  `ARROW:extension:name` values, since some consumers reject the entire schema.
- Spatial columns as WKB binary with `mariadb.logical_arrow_type=geoarrow.wkb`.
  The driver removes
  MariaDB's internal four-byte SRID prefix while reading and uses
  `ST_GeomFromWKB` during bulk ingest.
- `JSON` as the standard Arrow `arrow.json` extension type. Because MariaDB
  stores JSON as `LONGTEXT`, `GetTableSchema` recognizes its `JSON_VALID`
  constraint to recover the logical type.
- `ENUM` and `SET` as strings. Their declared values are JSON arrays in the
  `mariadb.enum_values` and `mariadb.set_values` field metadata.
- Unsigned integers as Arrow unsigned integers, including `BIGINT UNSIGNED`
  as `uint64`. `DECIMAL` values up to precision 38 use the narrowest Arrow
  decimal type. Wider decimals are lossless UTF-8 strings with precision,
  scale, `mariadb.logical_arrow_type`, and
  `mariadb.arrow_fallback=string` metadata, avoiding consumer failures on
  `decimal256` schemas.
- `BIT(n)` as binary with its bit width in `sql.length`, and `YEAR` as `int16`.
  Binary collations remain Arrow binary while textual collations remain UTF-8.
- `TIME` as an Arrow duration, preserving MariaDB's negative values and values
  up to 838 hours. `DATE`, `DATETIME`, and `TIMESTAMP` retain fractional-second
  precision; zero dates follow `mariadb.query.zero_datetime_behavior`.

MariaDB's result-set protocol sometimes reports logical aliases using their
storage types: notably `UUID` as `CHAR` and `JSON` as `LONGTEXT`. For arbitrary
SQL query results the driver does not guess from values. Cast at the consumer
or use `GetTableSchema` when native logical types are required.

### Parameter schema discovery

Prepared statements support `GetParameterSchema`. The returned Arrow schema
contains one field for each positional `?` parameter, in binding order. The
placeholder parser ignores question marks inside quoted strings, quoted
identifiers, and SQL comments.

MariaDB's prepared-statement protocol reports the number of parameters but
does not expose their inferred definitions through `database/sql`. Parameter
fields are therefore unnamed, nullable Arrow `null` fields. Applications can
use the schema to discover parameter count and order, but must still obtain
the intended parameter types from application or query context.

The statement must be prepared before requesting its parameter schema:

```python
from adbc_driver_manager import dbapi

with dbapi.connect(driver="mariadb", uri="mariadb://root@localhost/demo") as conn:
    with conn.cursor() as cursor:
        sql = "SELECT CAST(? AS SIGNED), CAST(? AS CHAR)"
        parameter_schema = cursor.adbc_prepare(sql)
        print(parameter_schema)

        cursor.execute(sql, [42, "MariaDB"])
        print(cursor.fetch_arrow_table())
```

### Transactions and isolation levels

Connections use autocommit by default at the ADBC API level. Set
`adbc.connection.autocommit` to `false` to group subsequent statements into a
transaction, then use `Commit` or `Rollback` to finish it. MariaDB implicitly
starts the next transaction when another statement executes while autocommit
remains disabled. Commit and rollback return an invalid-state error while
autocommit is enabled.

The standard `adbc.connection.transaction.isolation_level` option supports:

- `adbc.connection.transaction.isolation.default`
- `adbc.connection.transaction.isolation.read_uncommitted`
- `adbc.connection.transaction.isolation.read_committed`
- `adbc.connection.transaction.isolation.repeatable_read`
- `adbc.connection.transaction.isolation.serializable`

The option applies to the dedicated MariaDB session used by the ADBC
connection and can also be read to obtain the active level. Snapshot and
linearizable isolation are not supported by MariaDB and return ADBC
`NOT_IMPLEMENTED`. Change the isolation level before starting a transaction;
MariaDB does not allow transaction characteristics to change while a
transaction is active.

```python
from adbc_driver_manager import ConnectionOptions, dbapi

option = ConnectionOptions.ISOLATION_LEVEL.value
level = "adbc.connection.transaction.isolation.serializable"

with dbapi.connect(
    driver="mariadb",
    uri="mariadb://root@localhost/demo",
    autocommit=False,
) as conn:
    conn.adbc_connection.set_options(**{option: level})
    print(conn.adbc_connection.get_option(option))

    with conn.cursor() as cursor:
        cursor.execute("UPDATE accounts SET balance = balance - 10 WHERE id = 1")
    conn.commit()
```

### Object metadata

`GetObjects` exposes MariaDB table metadata through the standard ADBC nested
object schema. Column metadata includes type name, size, decimal digits,
numeric radix, character octet length, nullability, default value, comment,
auto-increment status, and generated-column status. Primary-key, unique,
foreign-key, and check constraints are returned in `table_constraints`;
foreign keys include the referenced catalog, table, and column for every key
column in ordinal order.

ADBC has no standard field for invisible columns. The driver preserves that
MariaDB attribute in the column remarks using the marker
`[MariaDB: INVISIBLE]`, appended after any user-defined column comment.
Check-constraint expressions likewise have no field in the standard ADBC
constraint structure; the driver exposes their name and `CHECK` type and uses
`JSON_VALID(column)` internally to recover MariaDB's logical JSON alias.

### Statistics

`GetStatistics` reports the standard ADBC row-count statistic from
`information_schema.TABLES` and distinct-count estimates for indexed columns
from `information_schema.STATISTICS`. It also exposes the MariaDB-specific
statistics `mariadb.statistic.data_length`,
`mariadb.statistic.index_length`, and
`mariadb.statistic.avg_row_length`; their keys are available through
`GetStatisticNames`.

MariaDB metadata values, including InnoDB row counts and index cardinalities,
are estimates. The driver therefore marks these statistics approximate even
when exact statistics are requested. It does not implicitly execute `ANALYZE
TABLE` or `ANALYZE FORMAT=JSON`, since those operations can scan data, update
persistent optimizer statistics, or execute the analyzed statement.

## Options

`adbc.connection.autocommit`
: **Values:** `true`, `false`. **Default:** `true`.

  Enable or disable MariaDB session autocommit. Enabling autocommit commits any
  active transaction, following MariaDB semantics.

`adbc.connection.transaction.isolation_level`
: **Values:** `default`, `read_uncommitted`, `read_committed`,
  `repeatable_read`, and `serializable`, using their full standardized ADBC
  option values listed above.

  Get or set the transaction isolation level for the MariaDB session.

`mariadb.query.zero_datetime_behavior`
: **Values:** `error`, `convert_to_null`. **Default:** `error`.

  Control what to do with DATE and TIMESTAMP values that contain zero components in the date (e.g. `0000-00-00`), which MariaDB allows for backwards compatibility. By default, this will error; `convert_to_null` will instead treat these values as equivalent to null.

## Compatibility

MariaDB databases are exposed as ADBC catalogs with an empty schema because
MariaDB has no namespace below a database. Schema-oriented clients may assign
an external name to that namespace; DuckDB exposes it as `main`, making tables
addressable as `<attachment>.main.<table>` while sending unqualified table
names to MariaDB.

When attaching through DuckDB's ADBC extension, specify MariaDB's backtick
identifier delimiters. Without this option DuckDB uses double quotes, causing
MariaDB table lookup to fail unless `ANSI_QUOTES` SQL mode is enabled.

```sql
ATTACH 'profile://mydb' AS mariadb (TYPE adbc, DELIMITER '``');
SELECT * FROM mariadb.main.my_table;
```

{{ compatibility_info|safe }}

{{ footnotes|safe }}

[mariadb]: https://mariadb.org/
