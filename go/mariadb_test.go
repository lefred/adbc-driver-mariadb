// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mariadb_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/adbc-drivers/driverbase-go/validation"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	mariadb "github.com/adbc-drivers/mariadb"
)

// MariaDBQuirks implements validation.DriverQuirks for MariaDB ADBC driver
type MariaDBQuirks struct {
	dsn string
	mem *memory.CheckedAllocator
}

func (q *MariaDBQuirks) SetupDriver(t *testing.T) driverbase.DriverWithContext {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return mariadb.NewDriver(q.mem)
}

func (q *MariaDBQuirks) TearDownDriver(t *testing.T, _ driverbase.DriverWithContext) {
	q.mem.AssertSize(t, 0)
}

func (q *MariaDBQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: q.dsn,
	}
}

func (q *MariaDBQuirks) CreateSampleTable(tableName string, r arrow.RecordBatch) error {
	// Use standard database/sql to create table directly
	db, err := sql.Open("mysql", q.dsn)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	// Drop table if it exists first to ensure clean state
	_, err = db.Exec("DROP TABLE IF EXISTS " + tableName)
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Build CREATE TABLE statement based on Arrow schema
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	createQuery.WriteString(tableName)
	createQuery.WriteString(" (")

	schema := r.Schema()
	for i, field := range schema.Fields() {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		createQuery.WriteString(field.Name)
		createQuery.WriteString(" ")

		// Map Arrow types to MariaDB types
		switch field.Type.ID() {
		case arrow.INT32:
			createQuery.WriteString("INT")
		case arrow.INT64:
			createQuery.WriteString("BIGINT")
		case arrow.STRING:
			createQuery.WriteString("VARCHAR(255)")
		case arrow.FLOAT32:
			createQuery.WriteString("FLOAT")
		case arrow.FLOAT64:
			createQuery.WriteString("DOUBLE")
		case arrow.BOOL:
			createQuery.WriteString("BOOLEAN")
		default:
			createQuery.WriteString("TEXT") // Default fallback
		}

		if !field.Nullable {
			createQuery.WriteString(" NOT NULL")
		}
	}
	createQuery.WriteString(")")

	_, err = db.Exec(createQuery.String())
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data from Arrow record
	if r.NumRows() > 0 {
		// Insert each row separately to handle NULL values correctly
		for row := range r.NumRows() {
			var insertQuery strings.Builder
			insertQuery.WriteString("INSERT INTO ")
			insertQuery.WriteString(tableName)
			insertQuery.WriteString(" VALUES (")

			values := make([]any, r.NumCols())
			for col := range r.NumCols() {
				column := r.Column(int(col))
				if column.IsNull(int(row)) {
					values[col] = nil
				} else {
					// Extract value based on column type
					switch arr := column.(type) {
					case *array.Int32:
						values[col] = arr.Value(int(row))
					case *array.Int64:
						values[col] = arr.Value(int(row))
					case *array.String:
						values[col] = arr.Value(int(row))
					case *array.Float32:
						values[col] = arr.Value(int(row))
					case *array.Float64:
						values[col] = arr.Value(int(row))
					case *array.Boolean:
						values[col] = arr.Value(int(row))
					default:
						values[col] = fmt.Sprintf("%v", column)
					}
				}
			}

			// Build placeholders and collect non-null values for prepared statement
			var queryParams []any
			for i, val := range values {
				if i > 0 {
					insertQuery.WriteString(", ")
				}
				if val == nil {
					insertQuery.WriteString("NULL")
				} else {
					insertQuery.WriteString("?")
					queryParams = append(queryParams, val)
				}
			}
			insertQuery.WriteString(")")

			_, err = db.Exec(insertQuery.String(), queryParams...)
			if err != nil {
				return fmt.Errorf("failed to insert row %d: %w", row, err)
			}
		}
	}

	return nil
}

func (q *MariaDBQuirks) DropTable(cnxn adbc.ConnectionWithContext, tblName string) error {
	ctx := context.Background()
	stmt, err := cnxn.NewStatement(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close(ctx))
	}()

	if err = stmt.SetSqlQuery(ctx, "DROP TABLE IF EXISTS "+tblName); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func (q *MariaDBQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	// Return metadata that matches what our MariaDB type converter actually returns
	metadata := map[string]string{}

	switch dt.ID() {
	case arrow.INT32:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "int"
		metadata["sql.precision"] = "10"
		metadata["sql.scale"] = "0"
	case arrow.INT64:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "bigint"
		metadata["sql.precision"] = "19"
		metadata["sql.scale"] = "0"
	case arrow.STRING:
		metadata["sql.column_name"] = "strings"
		metadata["sql.database_type_name"] = "varchar"
		metadata["sql.length"] = "255"
	case arrow.FLOAT32:
		metadata["sql.column_name"] = "floats"
		metadata["sql.database_type_name"] = "float"
	case arrow.FLOAT64:
		metadata["sql.column_name"] = "doubles"
		metadata["sql.database_type_name"] = "double"
	case arrow.BOOL:
		metadata["sql.column_name"] = "bools"
		metadata["sql.database_type_name"] = "tinyint"
	}

	return arrow.MetadataFrom(metadata)
}

func (q *MariaDBQuirks) Alloc() memory.Allocator      { return q.mem }
func (q *MariaDBQuirks) BindParameter(idx int) string { return "?" }
func (q *MariaDBQuirks) QuoteTableName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (q *MariaDBQuirks) SupportsBulkIngest(string) bool              { return true }
func (q *MariaDBQuirks) SupportsConcurrentStatements() bool          { return false }
func (q *MariaDBQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (q *MariaDBQuirks) SupportsExecuteSchema() bool                 { return true }
func (q *MariaDBQuirks) SupportsGetSetOptions() bool                 { return true }
func (q *MariaDBQuirks) SupportsGetTableSchema() bool                { return true }
func (q *MariaDBQuirks) SupportsPartitionedData() bool               { return false }
func (q *MariaDBQuirks) SupportsStatistics() bool                    { return true }
func (q *MariaDBQuirks) SupportsTransactions() bool                  { return true }
func (q *MariaDBQuirks) SupportsGetParameterSchema() bool            { return false }
func (q *MariaDBQuirks) SupportsDynamicParameterBinding() bool       { return true }
func (q *MariaDBQuirks) SupportsErrorIngestIncompatibleSchema() bool { return true }
func (q *MariaDBQuirks) Catalog() string                             { return "db" }
func (q *MariaDBQuirks) DBSchema() string                            { return "" }

func (q *MariaDBQuirks) GetMetadata(code adbc.InfoCode) any {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver Foundry Driver for MariaDB"
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorVersion:
		return "12.2.2-MariaDB (MariaDB Server)"
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "MariaDB"
	case adbc.InfoVendorSql:
		return true
	case adbc.InfoVendorSubstrait:
		return false
	case adbc.InfoCode(504): // SQL_IDENTIFIER_QUOTE_CHAR
		return "`"
	}
	return nil
}

func withQuirks(t *testing.T, fn func(*MariaDBQuirks)) {
	dsn := os.Getenv("MARIADB_DSN")
	if dsn == "" {
		t.Skip("Set MARIADB_DSN environment variable for validation tests")
	}

	q := &MariaDBQuirks{dsn: dsn}
	fn(q)
}

type MariaDBStatementTests struct {
	validation.StatementTests
}

func (s *MariaDBStatementTests) TestSqlIngestErrors() {
	s.T().Skip()
}

// TestValidation runs the comprehensive ADBC validation test suite
// This is the primary test that validates ADBC specification compliance
func TestValidation(t *testing.T) {
	withQuirks(t, func(q *MariaDBQuirks) {
		t.Run("Database", func(t *testing.T) {
			suite.Run(t, &validation.DatabaseTests{Quirks: q})
		})
		t.Run("Connection", func(t *testing.T) {
			suite.Run(t, &validation.ConnectionTests{Quirks: q})
		})
		t.Run("Statement", func(t *testing.T) {
			suite.Run(t, &validation.StatementTests{Quirks: q})
		})
	})
}

// -------------------- Additional Tests --------------------

type MariaDBTests struct {
	suite.Suite

	Quirks *MariaDBQuirks

	ctx    context.Context
	driver driverbase.DriverWithContext
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *MariaDBTests) SetupTest() {
	var err error
	s.ctx = context.Background()
	s.driver = s.Quirks.SetupDriver(s.T())
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, s.Quirks.DatabaseOptions())
	s.NoError(err)
	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)
	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *MariaDBTests) TearDownTest() {
	s.NoError(s.stmt.Close(s.ctx))
	s.NoError(s.cnxn.Close(s.ctx))
	s.Quirks.TearDownDriver(s.T(), s.driver)
	s.cnxn = nil
	s.NoError(s.db.Close(s.ctx))
	s.db = nil
	s.driver = nil
}

func (s *MariaDBTests) TestTransactions() {
	options := s.cnxn.(adbc.GetSetOptionsWithContext)

	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, "CREATE TEMPORARY TABLE adbc_transaction_test (value INT)"))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(options.SetOption(s.ctx, adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))

	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, "INSERT INTO adbc_transaction_test VALUES (1)"))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(s.cnxn.Rollback(s.ctx))
	s.EqualValues(0, s.transactionTestRowCount())

	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, "INSERT INTO adbc_transaction_test VALUES (2)"))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(s.cnxn.Commit(s.ctx))
	s.EqualValues(1, s.transactionTestRowCount())

	s.Require().NoError(options.SetOption(s.ctx, adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled))
}

func (s *MariaDBTests) transactionTestRowCount() int64 {
	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, "SELECT COUNT(*) FROM adbc_transaction_test"))
	rdr, _, err := s.stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	defer rdr.Release()
	s.Require().True(rdr.Next())
	s.Require().NoError(rdr.Err())
	return rdr.RecordBatch().Column(0).(*array.Int64).Value(0)
}

func (s *MariaDBTests) TestStatistics() {
	const table = "adbc_statistics_test"
	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, "DROP TABLE IF EXISTS "+table))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, "DROP TABLE IF EXISTS "+table))
		_, err := s.stmt.ExecuteUpdate(s.ctx)
		s.NoError(err)
	}()

	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx,
		"CREATE TABLE "+table+" (id INT PRIMARY KEY, category VARCHAR(20), INDEX(category))"))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx,
		"INSERT INTO "+table+" VALUES (1, 'a'), (2, 'a'), (3, 'b')"))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, "ANALYZE TABLE "+table))
	rdr, _, err := s.stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	rdr.Release()

	catalog, err := s.cnxn.(adbc.GetSetOptionsWithContext).GetOption(s.ctx, adbc.OptionKeyCurrentCatalog)
	s.Require().NoError(err)
	emptySchema := ""
	tablePattern := table
	statsReader, err := s.cnxn.(adbc.ConnectionGetStatistics).GetStatistics(
		s.ctx, &catalog, &emptySchema, &tablePattern, true)
	s.Require().NoError(err)
	defer statsReader.Release()
	s.Require().True(statsReader.Next())
	s.Require().NoError(statsReader.Err())

	stats := testutil.ExtractStatisticsForTable(statsReader.RecordBatch(), catalog, "", table)
	s.NotEmpty(stats)
	var foundRowCount, foundDistinct, foundDataLength bool
	for _, stat := range stats {
		s.True(stat.IsApproximate)
		switch stat.StatisticKey {
		case adbc.StatisticRowCountKey:
			foundRowCount = true
		case adbc.StatisticDistinctCountKey:
			foundDistinct = true
		case 1024:
			foundDataLength = true
		}
	}
	s.True(foundRowCount)
	s.True(foundDistinct)
	s.True(foundDataLength)

	namesReader, err := s.cnxn.(adbc.ConnectionGetStatistics).GetStatisticNames(s.ctx)
	s.Require().NoError(err)
	defer namesReader.Release()
	s.Require().True(namesReader.Next())
	s.EqualValues(3, namesReader.RecordBatch().NumRows())
	s.Equal("mariadb.statistic.data_length",
		namesReader.RecordBatch().Column(0).(*array.String).Value(0))
	s.EqualValues(1024, namesReader.RecordBatch().Column(1).(*array.Int16).Value(0))
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (s *MariaDBTests) TestSelect() {
	// Create test table with various MariaDB types including spatial
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		CREATE TEMPORARY TABLE test_types (
			bool_col TINYINT(1),
			tinyint_col TINYINT,
			int_col INT,
			bigint_col BIGINT,
			float_col FLOAT,
			double_col DOUBLE,
			varchar_col VARCHAR(100),
			json_col JSON,
			enum_col ENUM('active', 'inactive'),
			point_col POINT,
			polygon_col POLYGON,
			geometry_col GEOMETRY,
			bit_col BIT(8),
			utinyint_col TINYINT UNSIGNED,
			usmallint_col SMALLINT UNSIGNED,
			uint_col INT UNSIGNED,
			ubigint_col BIGINT UNSIGNED
		)
	`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Insert test data including spatial data
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		INSERT INTO test_types VALUES (
			1, 42, 12345, 9876543210, 3.25, 6.75, 'hello world',
			'{"key": "value", "number": 42}', 'active',
			ST_GeomFromText('POINT(1 2)'),
			ST_GeomFromText('POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))'),
			ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'),
			b'10101010',
			200, 60000, 3000000000, 10000000000000000000
		)
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	for _, testCase := range []selectCase{
		{
			name:  "boolean",
			query: "SELECT bool_col AS istrue FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "istrue",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "istrue",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"istrue": 1}]`,
		},
		{
			name:  "tinyint",
			query: "SELECT tinyint_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"value": 42}]`,
		},
		{
			name:  "int32",
			query: "SELECT int_col AS theanswer FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "INT",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 12345}]`,
		},
		{
			name:  "int64",
			query: "SELECT bigint_col AS theanswer FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "BIGINT",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 9876543210}]`,
		},
		{
			name:  "float32",
			query: "SELECT float_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "FLOAT",
						"sql.precision":          "9223372036854775807",
						"sql.scale":              "9223372036854775807",
					}),
				},
			}, nil),
			expected: `[{"value": 3.25}]`,
		},
		{
			name:  "float64",
			query: "SELECT double_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "DOUBLE",
						"sql.precision":          "9223372036854775807",
						"sql.scale":              "9223372036854775807",
					}),
				},
			}, nil),
			expected: `[{"value": 6.75}]`,
		},
		{
			name:  "string",
			query: "SELECT varchar_col AS greeting FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "greeting",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "greeting",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"greeting": "hello world"}]`,
		},
		{
			name:  "json",
			query: "SELECT json_col AS data FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "data",
					Type:     func() arrow.DataType { t, _ := extensions.NewJSONType(arrow.BinaryTypes.String); return t }(),
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "data",
						"sql.database_type_name": "JSON",
						"mariadb.native_type":    "json",
					}),
				},
			}, nil),
			expected: `[{"data": "{\"key\": \"value\", \"number\": 42}"}]`,
		},
		{
			name:  "enum",
			query: "SELECT enum_col AS status FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "status",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "status",
						"sql.database_type_name": "ENUM",
						"mariadb.is_enum_set":    "true",
					}),
				},
			}, nil),
			expected: `[{"status": "active"}]`,
		},
		{
			name:  "point",
			query: "SELECT point_col AS location FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "location",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":            "location",
						"sql.database_type_name":     "GEOMETRY",
						"mariadb.is_spatial":         "true",
						"mariadb.geometry_encoding":  "internal_srid_wkb",
						"mariadb.logical_arrow_type": "geoarrow.wkb",
					}),
				},
			}, nil),
			expected: `[{"location": "AQEAAAAAAAAAAADwPwAAAAAAAABA"}]`,
		},
		{
			name:  "polygon",
			query: "SELECT polygon_col AS area FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "area",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":            "area",
						"sql.database_type_name":     "GEOMETRY",
						"mariadb.is_spatial":         "true",
						"mariadb.geometry_encoding":  "internal_srid_wkb",
						"mariadb.logical_arrow_type": "geoarrow.wkb",
					}),
				},
			}, nil),
			expected: `[{"area": "AQMAAAABAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAhAAAAAAAAACEAAAAAAAAAIQAAAAAAAAAhAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}]`,
		},
		{
			name:  "geometry",
			query: "SELECT geometry_col AS shape FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "shape",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":            "shape",
						"sql.database_type_name":     "GEOMETRY",
						"mariadb.is_spatial":         "true",
						"mariadb.geometry_encoding":  "internal_srid_wkb",
						"mariadb.logical_arrow_type": "geoarrow.wkb",
					}),
				},
			}, nil),
			expected: `[{"shape": "AQIAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAAAAQAAAAAAAAABA"}]`,
		},
		{
			name:  "bit8",
			query: "SELECT bit_col AS bitvalue FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bitvalue",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bitvalue",
						"sql.database_type_name": "BIT",
					}),
				},
			}, nil),
			expected: `[{"bitvalue": "qg=="}]`,
		},
		{
			name:  "unsigned_tinyint",
			query: "SELECT utinyint_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Uint8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "TINYINT UNSIGNED",
					}),
				},
			}, nil),
			expected: `[{"value": 200}]`,
		},
		{
			name:  "unsigned_smallint",
			query: "SELECT usmallint_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Uint16,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "SMALLINT UNSIGNED",
					}),
				},
			}, nil),
			expected: `[{"value": 60000}]`,
		},
		{
			name:  "unsigned_int",
			query: "SELECT uint_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Uint32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "INT UNSIGNED",
					}),
				},
			}, nil),
			expected: `[{"value": 3000000000}]`,
		},
		{
			name:  "unsigned_bigint",
			query: "SELECT ubigint_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Uint64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "BIGINT UNSIGNED",
					}),
				},
			}, nil),
			expected: `[{"value": 10000000000000000000}]`,
		},
	} {
		s.Run(testCase.name, func() {
			s.NoError(s.stmt.SetSqlQuery(s.ctx, testCase.query))

			rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
			s.NoError(err)
			defer rdr.Release()

			s.Truef(testCase.schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", testCase.schema, rdr.Schema())
			s.Equal(int64(-1), rows)
			s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

			expectedRecord, _, err := array.RecordFromJSON(s.Quirks.Alloc(), testCase.schema, bytes.NewReader([]byte(testCase.expected)))
			s.NoError(err)
			defer expectedRecord.Release()

			rec := rdr.RecordBatch()
			s.NotNil(rec)

			s.Truef(array.RecordEqual(expectedRecord, rec), "expected: %s\ngot: %s", expectedRecord, rec)

			s.False(rdr.Next())
			s.NoError(rdr.Err())
		})
	}
}

type MariaDBTestSuite struct {
	suite.Suite
	dsn    string
	mem    *memory.CheckedAllocator
	ctx    context.Context
	driver driverbase.DriverWithContext
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *MariaDBTestSuite) SetupSuite() {
	var err error
	s.dsn = os.Getenv("MARIADB_DSN")
	if s.dsn == "" {
		s.T().Skip("Set MARIADB_DSN environment variable")
	}

	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	s.driver = mariadb.NewDriver(s.mem)
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, map[string]string{
		adbc.OptionKeyURI: s.dsn,
	})
	s.NoError(err)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *MariaDBTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close(s.ctx))
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close(s.ctx))
	}
	if s.db != nil {
		s.NoError(s.db.Close(s.ctx))
	}
	s.mem.AssertSize(s.T(), 0)
}

func (s *MariaDBTestSuite) TestBulkIngestManyColumns() {
	const numCols = 100
	const numRows = 5
	tableName := "bulk_ingest_wide"

	// Drop the table if it exists
	s.NoError(s.stmt.SetSqlQuery(s.ctx, "DROP TABLE IF EXISTS `"+tableName+"`"))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)

	// Build a schema with 100 int64 columns
	fields := make([]arrow.Field, numCols)
	for i := range numCols {
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("col_%d", i), Type: arrow.PrimitiveTypes.Int64, Nullable: true,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// Build a record batch with a few rows
	batchbldr := array.NewRecordBuilder(s.mem, schema)
	defer batchbldr.Release()
	for col := range numCols {
		bldr := batchbldr.Field(col).(*array.Int64Builder)
		for row := range numRows {
			bldr.Append(int64(col*numRows + row))
		}
	}
	batch := batchbldr.NewRecordBatch()
	defer batch.Release()

	// Ingest — this would fail before the fix because
	// 1000 (default batch size) * 100 columns = 100,000 placeholders > 65,535 limit
	stmt, err := s.cnxn.NewStatement(s.ctx)
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close(s.ctx)) }()

	s.Require().NoError(stmt.SetOption(s.ctx, adbc.OptionKeyIngestTargetTable, tableName))
	s.Require().NoError(stmt.Bind(s.ctx, batch))

	affected, err := stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 {
		s.EqualValues(numRows, affected)
	}

	// Verify the data was ingested correctly
	s.Require().NoError(stmt.SetSqlQuery(s.ctx, "SELECT COUNT(*) FROM `"+tableName+"`"))
	rdr, _, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	defer rdr.Release()

	s.Require().True(rdr.Next())
	count := rdr.RecordBatch().Column(0).(*array.Int64).Value(0)
	s.EqualValues(numRows, count)
}

func (s *MariaDBTestSuite) TestZeroDatetimeBehaviorOptions() {
	db, err := s.driver.NewDatabaseWithContext(s.ctx, map[string]string{
		adbc.OptionKeyURI:                     s.dsn,
		mariadb.OptionKeyZeroDatetimeBehavior: mariadb.OptionValueZeroDatetimeBehaviorConvertToNull,
	})
	s.Require().NoError(err)
	defer func() { s.NoError(db.Close(s.ctx)) }()

	dbOptions := db.(adbc.GetSetOptionsWithContext)
	value, err := dbOptions.GetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior)
	s.Require().NoError(err)
	s.Equal(mariadb.OptionValueZeroDatetimeBehaviorError, value)

	cnxn, err := db.Open(s.ctx)
	s.Require().NoError(err)
	defer func() { s.NoError(cnxn.Close(s.ctx)) }()

	cnxnOptions := cnxn.(adbc.GetSetOptionsWithContext)
	value, err = cnxnOptions.GetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior)
	s.Require().NoError(err)
	s.Equal(mariadb.OptionValueZeroDatetimeBehaviorError, value)

	stmt, err := cnxn.NewStatement(s.ctx)
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close(s.ctx)) }()

	stmtOptions := stmt.(adbc.GetSetOptionsWithContext)
	value, err = stmtOptions.GetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior)
	s.Require().NoError(err)
	s.Equal(mariadb.OptionValueZeroDatetimeBehaviorError, value)

	s.Require().NoError(cnxnOptions.SetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior, mariadb.OptionValueZeroDatetimeBehaviorConvertToNull))
	stmtAfterConnectionChange, err := cnxn.NewStatement(s.ctx)
	s.Require().NoError(err)
	defer func() { s.NoError(stmtAfterConnectionChange.Close(s.ctx)) }()

	value, err = stmtAfterConnectionChange.(adbc.GetSetOptionsWithContext).GetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior)
	s.Require().NoError(err)
	s.Equal(mariadb.OptionValueZeroDatetimeBehaviorConvertToNull, value)

	s.Require().NoError(stmtOptions.SetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior, mariadb.OptionValueZeroDatetimeBehaviorConvertToNull))
	value, err = stmtOptions.GetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior)
	s.Require().NoError(err)
	s.Equal(mariadb.OptionValueZeroDatetimeBehaviorConvertToNull, value)

	value, err = cnxnOptions.GetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior)
	s.Require().NoError(err)
	s.Equal(mariadb.OptionValueZeroDatetimeBehaviorConvertToNull, value)
}

func (s *MariaDBTestSuite) TestZeroDatetimeBehaviorInvalidOptionValues() {
	dbOptions := s.db.(adbc.GetSetOptionsWithContext)
	cnxnOptions := s.cnxn.(adbc.GetSetOptionsWithContext)
	stmtOptions := s.stmt.(adbc.GetSetOptionsWithContext)

	err := dbOptions.SetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior, "invalid")
	s.Require().Error(err)
	var adbcErr adbc.Error
	s.Require().Truef(errors.As(err, &adbcErr), "expected ADBC error, got %T: %v", err, err)
	s.Equal(adbc.StatusInvalidArgument, adbcErr.Code)

	err = cnxnOptions.SetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior, "invalid")
	s.Require().Error(err)
	adbcErr = adbc.Error{}
	s.Require().Truef(errors.As(err, &adbcErr), "expected ADBC error, got %T: %v", err, err)
	s.Equal(adbc.StatusInvalidArgument, adbcErr.Code)

	err = stmtOptions.SetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior, "invalid")
	s.Require().Error(err)
	adbcErr = adbc.Error{}
	s.Require().Truef(errors.As(err, &adbcErr), "expected ADBC error, got %T: %v", err, err)
	s.Equal(adbc.StatusInvalidArgument, adbcErr.Code)
}

func (s *MariaDBTestSuite) TestZeroDatetimeBehaviorQuery() {
	stmt, err := s.cnxn.NewStatement(s.ctx)
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close(s.ctx)) }()

	s.Require().NoError(stmt.SetSqlQuery(s.ctx, "SET @adbc_old_sql_mode = @@SESSION.sql_mode"))
	_, err = stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(stmt.SetSqlQuery(s.ctx, "SET SESSION sql_mode = ''"))
	_, err = stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.NoError(stmt.SetSqlQuery(s.ctx, "SET SESSION sql_mode = @adbc_old_sql_mode"))
		_, err := stmt.ExecuteUpdate(s.ctx)
		s.NoError(err)
	}()

	s.Require().NoError(stmt.SetSqlQuery(s.ctx, "DROP TEMPORARY TABLE IF EXISTS zero_datetime_values"))
	_, err = stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.NoError(stmt.SetSqlQuery(s.ctx, "DROP TEMPORARY TABLE IF EXISTS zero_datetime_values"))
		_, err := stmt.ExecuteUpdate(s.ctx)
		s.NoError(err)
	}()

	s.Require().NoError(stmt.SetSqlQuery(s.ctx, `
		CREATE TEMPORARY TABLE zero_datetime_values (
			id INT NOT NULL,
			date_col DATE NULL,
			datetime_col DATETIME NULL
		)
	`))
	_, err = stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)

	s.Require().NoError(stmt.SetSqlQuery(s.ctx, `
		INSERT INTO zero_datetime_values (id, date_col, datetime_col) VALUES
			(0, '0000-00-00', '0000-00-00 03:04:05'),
			(1, '0000-01-02', '0000-01-02 03:04:05'),
			(2, '2026-00-02', '2026-00-02 03:04:05'),
			(3, '2026-07-00', '2026-07-00 03:04:05'),
			(4, '2026-07-08', '2026-07-08 12:34:56')
	`))
	_, err = stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)

	s.Require().NoError(stmt.SetSqlQuery(s.ctx, "SELECT date_col FROM zero_datetime_values ORDER BY id"))
	rdr, _, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	defer rdr.Release()

	s.False(rdr.Next())
	err = rdr.Err()
	s.Require().Error(err)
	var adbcErr adbc.Error
	s.Require().Truef(errors.As(err, &adbcErr), "expected ADBC error, got %T: %v", err, err)
	s.Equal(adbc.StatusInvalidData, adbcErr.Code)

	s.Require().NoError(stmt.SetOption(s.ctx, mariadb.OptionKeyZeroDatetimeBehavior, mariadb.OptionValueZeroDatetimeBehaviorConvertToNull))
	s.Require().NoError(stmt.SetSqlQuery(s.ctx, "SELECT date_col, datetime_col FROM zero_datetime_values ORDER BY id"))
	rdr, _, err = stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	defer rdr.Release()

	s.Require().True(rdr.Next())
	rec := rdr.RecordBatch()
	s.Require().EqualValues(5, rec.NumRows())

	dateCol := rec.Column(0).(*array.Date32)
	datetimeCol := rec.Column(1).(*array.Timestamp)
	for i := range 4 {
		s.True(dateCol.IsNull(i))
		s.True(datetimeCol.IsNull(i))
	}

	s.False(dateCol.IsNull(4))
	s.Equal(arrow.Date32FromTime(time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)), dateCol.Value(4))

	s.False(datetimeCol.IsNull(4))
	timestampType := datetimeCol.DataType().(*arrow.TimestampType)
	s.Equal(time.Date(2026, 7, 8, 12, 34, 56, 0, time.UTC), datetimeCol.Value(4).ToTime(timestampType.Unit))

	s.False(rdr.Next())
	s.NoError(rdr.Err())
}

func TestMariaDBTypeTests(t *testing.T) {
	dsn := os.Getenv("MARIADB_DSN")
	if dsn == "" {
		t.Skip("Set MARIADB_DSN environment variable for type tests")
	}

	quirks := &MariaDBQuirks{dsn: dsn}
	suite.Run(t, &MariaDBTests{Quirks: quirks})
}

func TestMariaDBIntegrationSuite(t *testing.T) {
	suite.Run(t, new(MariaDBTestSuite))
}

// TestURIParsing tests the parseToMariaDBDSN function with various URI formats
func TestURIParsing(t *testing.T) {
	factory := mariadb.NewMariaDBDBFactory()

	tests := []struct {
		name          string
		mariadbURI    string
		username      string
		password      string
		expectedDSN   string
		shouldError   bool
		errorContains string
	}{
		// TCP connection variations
		{
			name:        "basic tcp with port",
			mariadbURI:  "mariadb://user:pass@localhost:3306/testdb",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:        "tcp without port - should default to 3306",
			mariadbURI:  "mariadb://user:pass@localhost/testdb",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:          "tcp without host - should be invalid",
			mariadbURI:    "mariadb://user:pass@/testdb",
			shouldError:   true,
			errorContains: "missing hostname in URI",
		},
		{
			name:        "tcp without database",
			mariadbURI:  "mariadb://user:pass@localhost:3306",
			expectedDSN: "user:pass@tcp(localhost:3306)/",
		},
		{
			name:        "tcp without database but with slash",
			mariadbURI:  "mariadb://user:pass@localhost:3306/",
			expectedDSN: "user:pass@tcp(localhost:3306)/",
		},
		{
			name:        "tcp with custom port",
			mariadbURI:  "mariadb://user:pass@example.com:3307/myapp",
			expectedDSN: "user:pass@tcp(example.com:3307)/myapp",
		},
		{
			name:        "tcp with ip address",
			mariadbURI:  "mariadb://user:pass@127.0.0.1:3306/testdb",
			expectedDSN: "user:pass@tcp(127.0.0.1:3306)/testdb",
		},
		{
			name:        "tcp with ipv6 host",
			mariadbURI:  "mariadb://user:pass@[::1]:3306/testdb",
			expectedDSN: "user:pass@tcp([::1]:3306)/testdb",
		},
		{
			name:        "tcp with ipv6 host, default port",
			mariadbURI:  "mariadb://user:pass@[::1]/testdb",
			expectedDSN: "user:pass@tcp([::1]:3306)/testdb",
		},

		// Credential handling variations
		{
			name:        "no credentials in uri",
			mariadbURI:  "mariadb://localhost:3306/testdb",
			expectedDSN: "tcp(localhost:3306)/testdb",
		},
		{
			name:        "only username in uri",
			mariadbURI:  "mariadb://user@localhost:3306/testdb",
			expectedDSN: "user@tcp(localhost:3306)/testdb",
		},
		{
			name:        "override credentials with options",
			mariadbURI:  "mariadb://olduser:oldpass@localhost:3306/testdb",
			username:    "newuser",
			password:    "newpass",
			expectedDSN: "newuser:newpass@tcp(localhost:3306)/testdb",
		},
		{
			name:        "add credentials via options",
			mariadbURI:  "mariadb://localhost:3306/testdb",
			username:    "admin",
			password:    "secret",
			expectedDSN: "admin:secret@tcp(localhost:3306)/testdb",
		},
		{
			name:        "override only username",
			mariadbURI:  "mariadb://user:pass@localhost:3306/testdb",
			username:    "newuser",
			expectedDSN: "newuser:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:        "override only password",
			mariadbURI:  "mariadb://user:pass@localhost:3306/testdb",
			password:    "newpass",
			expectedDSN: "user:newpass@tcp(localhost:3306)/testdb",
		},

		// Query parameter variations
		{
			name:        "single query parameter",
			mariadbURI:  "mariadb://user:pass@localhost:3306/testdb?charset=utf8mb4",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb?charset=utf8mb4",
		},
		{
			name:        "multiple query parameters",
			mariadbURI:  "mariadb://user:pass@localhost:3306/testdb?charset=utf8mb4&timeout=30s&tls=false",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb?charset=utf8mb4&timeout=30s&tls=false",
		},
		{
			name:        "ssl parameters",
			mariadbURI:  "mariadb://user:pass@localhost:3306/testdb?tls=skip-verify&timeout=10s",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb?tls=skip-verify&timeout=10s",
		},
		{
			name:        "url encoded database name",
			mariadbURI:  "mariadb://user:pass@localhost:3306/test%20db?charset=utf8",
			expectedDSN: "user:pass@tcp(localhost:3306)/test%20db?charset=utf8",
		},
		{
			name:        "query parameters with encoding",
			mariadbURI:  "mariadb://user:pass@localhost/testdb?time_zone=%27%2B00%3A00%27",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb?time_zone=%27%2B00%3A00%27",
		},

		// Unix socket variations
		{
			name:        "unix socket with parentheses",
			mariadbURI:  "mariadb://user:pass@(/tmp/mariadb.sock)/testdb",
			expectedDSN: "user:pass@unix(/tmp/mariadb.sock)/testdb",
		},
		{
			name:          "unix socket with percent encoding - should be invalid. Must use parenthesis",
			mariadbURI:    "mariadb://user:pass@/tmp%2Fmariadb.sock/testdb",
			shouldError:   true,
			errorContains: "missing hostname in URI",
		},
		{
			name:        "unix socket with complex path",
			mariadbURI:  "mariadb://user:pass@(/var/run/mariadbd/mariadbd.sock)/myapp",
			expectedDSN: "user:pass@unix(/var/run/mariadbd/mariadbd.sock)/myapp",
		},
		{
			name:        "unix socket without database",
			mariadbURI:  "mariadb://user:pass@(/tmp/mariadb.sock)",
			expectedDSN: "user:pass@unix(/tmp/mariadb.sock)/",
		},
		{
			name:        "unix socket with query params",
			mariadbURI:  "mariadb://user:pass@(/tmp/mariadb.sock)/testdb?charset=utf8mb4",
			expectedDSN: "user:pass@unix(/tmp/mariadb.sock)/testdb?charset=utf8mb4",
		},
		{
			name:          "unix socket with empty host (ambiguous) - should be invalid",
			mariadbURI:    "mariadb://user:pass@/tmp/mariadb.sock/testdb",
			shouldError:   true,
			errorContains: "missing hostname in URI",
		},
		{
			name:          "invalid unix socket (missing parenthesis)",
			mariadbURI:    "mariadb://user@(/tmp/mariadb.sock/testdb",
			shouldError:   true,
			errorContains: "missing closing ')'",
		},
		{
			name:        "unix socket (paren) with encoded db name",
			mariadbURI:  "mariadb://user:pass@(/tmp/mariadb.sock)/my%20db?foo=bar",
			expectedDSN: "user:pass@unix(/tmp/mariadb.sock)/my%20db?foo=bar",
		},
		// Special characters and edge cases
		{
			name:        "credentials with special characters",
			mariadbURI:  "mariadb://my%40user:p%40ss%24word@localhost:3306/testdb",
			expectedDSN: "my@user:p@ss$word@tcp(localhost:3306)/testdb",
		},

		// Error cases
		{
			name:          "invalid mariadb uri format",
			mariadbURI:    "mariadb://[invalid-uri",
			shouldError:   true,
			errorContains: "invalid MariaDB URI format",
		},
		{
			name:          "mysql scheme is rejected",
			mariadbURI:    "mysql://user:pass@localhost:3306/testdb",
			shouldError:   true,
			errorContains: `unsupported URI scheme "mysql": expected mariadb://`,
		},
		{
			name:          "invalid socket path encoding",
			mariadbURI:    "mariadb://user:pass@%ZZ%invalid/testdb",
			shouldError:   true,
			errorContains: "invalid MariaDB URI format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := map[string]string{
				adbc.OptionKeyURI: tt.mariadbURI,
			}
			if tt.username != "" {
				opts[adbc.OptionKeyUsername] = tt.username
			}
			if tt.password != "" {
				opts[adbc.OptionKeyPassword] = tt.password
			}

			result, err := factory.BuildMariaDBDSN(opts)

			if tt.shouldError {
				require.ErrorContains(t, err, tt.errorContains)
				return
			}

			require.NoError(t, err, "unexpected error")
			assert.Equal(t, tt.expectedDSN, result, "DSN should match expected value")
		})
	}
}
