<!--
  Copyright (c) 2025 ADBC Drivers Contributors
  Copyright (c) 2026 lefred - MariaDB Foundation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ADBC Driver for MariaDB

Afiliated with the MariaDB Foundation.


## Installation

Pre-packaged builds of the drivers in this repo have been made available for
various platforms from the [Columnar](https://columnar.tech) CDN. These can be
installed by any tool that supports [ADBC](https://arrow.apache.org/adbc/)
Driver Manifests, such as [dbc](https://columnar.tech/dbc):

```sh
dbc install --no-verify <path to>/mariadb_linux_amd64_v0.1.0.tar.gz

dbc list
DRIVER  VERSION LEVEL LOCATION                       
                                                     
mariadb 0.1.0   user  /home/fred/.config/adbc/drivers
```

## Usage with DuckDB

Create the adbc profile for your MariaDB Server:

```
$ cat /home/fred/.config/adbc/profiles/mydb.toml
profile_version = 1
driver = "mariadb"

[Options]
uri="mariadb://msandbox:msandbox@127.0.0.1:13100/test"
```

You can now test it with DuckDB:

```
$ duckdb 
DuckDB v1.5.4 (Variegata)
Enter ".help" for usage hints.
memroy D INSTALL adbc FROM community;
memory D LOAD adbc;
memory D ATTACH 'profile://mydb' AS mariadb (
               TYPE adbc,
               DELIMITER '``'
           );
memory D SHOW ALL TABLES;
┌──────────┬─────────┬────────────┬──────────────────────────────┬───────────────────────────────┬───────────┐
│ database │ schema  │    name    │         column_names         │         column_types          │ temporary │
│ varchar  │ varchar │  varchar   │          varchar[]           │           varchar[]           │  boolean  │
├──────────┼─────────┼────────────┼──────────────────────────────┼───────────────────────────────┼───────────┤
│ mariadb  │ main    │ rich_types │ [id, embedding, ipv4, ipv6,  │ [VARCHAR, BLOB, VARCHAR,      │ false     │
│          │         │            │  location, area, document,   │  VARCHAR, BLOB, BLOB,         │           │
│          │         │            │  status, flags,              │  VARCHAR, VARCHAR, VARCHAR,   │           │
│          │         │            │  unsigned_value,             │  UBIGINT, VARCHAR, INTERVAL,  │           │
│          │         │            │  precise_value, elapsed,     │  DATE, TIMESTAMP,             │           │
│          │         │            │  event_date, event_datetime, │  TIMESTAMP WITH TIME ZONE,    │           │
│          │         │            │  event_timestamp, bits,      │  BLOB, SMALLINT, BLOB,        │           │
│          │         │            │  event_year, binary_value,   │  VARCHAR]                     │           │
│          │         │            │  textual_value]              │                               │           │
├──────────┼─────────┼────────────┼──────────────────────────────┼───────────────────────────────┼───────────┤
│ mariadb  │ main    │ t1         │ [id, name]                   │ [INTEGER, VARCHAR]            │ false     │
├──────────┼─────────┼────────────┼──────────────────────────────┼───────────────────────────────┼───────────┤
│ mariadb  │ main    │ t2         │ [id, name, created]          │ [VARCHAR, VARCHAR,            │ false     │
│          │         │            │                              │  TIMESTAMP WITH TIME ZONE]    │           │
└──────────┴─────────┴────────────┴──────────────────────────────┴───────────────────────────────┴───────────┘
memory D SELECT * FROM mariadb.main.t2;
┌──────────────────────────────────────┬─────────────┬──────────────────────────┐
│                  id                  │     name    │         created          │
│               varchar                │   varchar   │ timestamp with time zone │
├──────────────────────────────────────┼─────────────┼──────────────────────────┤
│ 019f6239-5ae2-77e0-a8e5-ee4bd2bf9158 │ anna        │ 2026-07-14 22:02:33+02   │
│ 019f6239-5ae2-78c0-a082-05c1bec9731a │ kaj         │ 2026-07-14 22:02:33+02   │
│ 019f6239-5ae2-78e8-94b2-69213c46af07 │ roman       │ 2026-07-14 22:02:33+02   │
│ 019f6246-e633-7978-af54-ad773ecd5e8e │ fred        │ 2026-07-14 22:17:21+02   │
│ 019f6246-e633-7978-af54-ad773ecd5e90 │ new name    │ 2026-07-14 23:34:04+02   │
└──────────────────────────────────────┴─────────────┴──────────────────────────┘
```

