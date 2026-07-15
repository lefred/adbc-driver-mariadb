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

package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	// Default num of rows per batch for batched INSERT
	MariaDBDefaultIngestBatchSize = 1000
	// MariaDB's maximum number of placeholders in a prepared statement
	MariaDBMaxPlaceholders = 65535
)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *mariadbConnectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	if err := c.ClearPending(); err != nil {
		return "", err
	}
	var database string
	err := c.Conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database)
	if err != nil {
		return "", c.ErrorHelper.WrapIO(err, "failed to get current database")
	}
	if database == "" {
		return "", c.ErrorHelper.InvalidState("no current database set")
	}
	return database, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *mariadbConnectionImpl) GetCurrentDbSchema(_ context.Context) (string, error) {
	return "", nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *mariadbConnectionImpl) SetCurrentCatalog(ctx context.Context, catalog string) error {
	if err := c.ClearPending(); err != nil {
		return err
	}
	_, err := c.Conn.ExecContext(ctx, "USE "+quoteIdentifier(catalog))
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *mariadbConnectionImpl) SetCurrentDbSchema(_ context.Context, schema string) error {
	if schema != "" {
		return c.ErrorHelper.InvalidArgument("cannot set schema in MariaDB: schemas are not supported")
	}
	return nil
}

func (c *mariadbConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if err := c.ClearPending(); err != nil {
		return err
	}
	if c.version == "" {
		var version, comment string
		if err := c.Conn.QueryRowContext(ctx, "SELECT @@version, @@version_comment").Scan(&version, &comment); err != nil {
			return c.ErrorHelper.WrapIO(err, "failed to get version")
		}
		c.version = fmt.Sprintf("%s (%s)", version, comment)
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}

// GetTableSchema returns the Arrow schema for a MariaDB table
func (c *mariadbConnectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (schema *arrow.Schema, err error) {
	if err := c.ClearPending(); err != nil {
		return nil, err
	}
	// Struct to capture MariaDB column information
	type tableColumn struct {
		OrdinalPosition        int32
		ColumnName             string
		DataType               string
		ColumnType             string
		IsNullable             string
		CharacterMaximumLength sql.NullInt64
		NumericPrecision       sql.NullInt64
		NumericScale           sql.NullInt64
		DatetimePrecision      sql.NullInt64
		CharacterSetName       sql.NullString
		CollationName          sql.NullString
	}

	query := `SELECT
		ORDINAL_POSITION,
		COLUMN_NAME,
		DATA_TYPE,
		COLUMN_TYPE,
		IS_NULLABLE,
		CHARACTER_MAXIMUM_LENGTH,
		NUMERIC_PRECISION,
		NUMERIC_SCALE,
		DATETIME_PRECISION,
		CHARACTER_SET_NAME,
		COLLATION_NAME
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	ORDER BY ORDINAL_POSITION`

	var args []any
	var selectedCatalog string
	if catalog != nil && *catalog != "" {
		// Use specified catalog (database)
		selectedCatalog = *catalog
	} else {
		// Use current database
		currentDB, err := c.GetCurrentCatalog(ctx)
		if err != nil {
			return nil, err
		}
		selectedCatalog = currentDB
	}
	args = []any{selectedCatalog, tableName}

	// Execute query to get column information
	rows, err := c.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query table schema")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	var columns []tableColumn
	for rows.Next() {
		var col tableColumn
		err := rows.Scan(
			&col.OrdinalPosition,
			&col.ColumnName,
			&col.DataType,
			&col.ColumnType,
			&col.IsNullable,
			&col.CharacterMaximumLength,
			&col.NumericPrecision,
			&col.NumericScale,
			&col.DatetimePrecision,
			&col.CharacterSetName,
			&col.CollationName,
		)
		if err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan column information")
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "rows error")
	}
	if err := rows.Close(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to close table schema rows")
	}

	if len(columns) == 0 {
		return nil, c.ErrorHelper.NotFound("table not found: %s", tableName)
	}

	// MariaDB implements JSON as LONGTEXT plus a JSON_VALID check constraint.
	// Recover the logical JSON type for metadata-based schema discovery. A lack
	// of permission to inspect constraints must not make the table unreadable.
	jsonColumns := make(map[string]bool)
	checkRows, checkErr := c.Conn.QueryContext(ctx, `SELECT cc.CHECK_CLAUSE
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
		  ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
		 AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
		WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
		  AND tc.CONSTRAINT_TYPE = 'CHECK'`, selectedCatalog, tableName)
	if checkErr == nil {
		for checkRows.Next() {
			var clause string
			if scanErr := checkRows.Scan(&clause); scanErr != nil {
				_ = checkRows.Close()
				return nil, c.ErrorHelper.WrapIO(scanErr, "failed to scan check constraint")
			}
			lowerClause := strings.ToLower(clause)
			for _, col := range columns {
				quotedName := "`" + strings.ToLower(strings.ReplaceAll(col.ColumnName, "`", "``")) + "`"
				if strings.Contains(lowerClause, "json_valid("+quotedName+")") ||
					strings.Contains(lowerClause, "json_valid("+strings.ToLower(col.ColumnName)+")") {
					jsonColumns[col.ColumnName] = true
				}
			}
		}
		if rowErr := checkRows.Err(); rowErr != nil {
			_ = checkRows.Close()
			return nil, c.ErrorHelper.WrapIO(rowErr, "failed to read check constraints")
		}
		_ = checkRows.Close()
	}

	// Build Arrow schema from column information using type converter
	typeConverter := makeTypeConverter(c.zeroDatetimeBehavior)
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		// Create ColumnType struct for the type converter
		var length, precision, scale *int64
		if col.CharacterMaximumLength.Valid {
			length = &col.CharacterMaximumLength.Int64
		}
		if col.NumericPrecision.Valid {
			precision = &col.NumericPrecision.Int64
		}
		if col.NumericScale.Valid {
			scale = &col.NumericScale.Int64
		}

		// Use DATA_TYPE but append UNSIGNED if COLUMN_TYPE indicates it
		// Only check integer types to avoid false positives with enum/set value lists
		dbTypeName := col.DataType
		if jsonColumns[col.ColumnName] {
			dbTypeName = "JSON"
		}
		switch strings.ToUpper(col.DataType) {
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT":
			if strings.Contains(strings.ToUpper(col.ColumnType), "UNSIGNED") {
				dbTypeName = col.DataType + " UNSIGNED"
			}
		}
		// DATETIME_PRECISION, not NUMERIC_PRECISION, controls temporal units.
		switch strings.ToUpper(col.DataType) {
		case "TIME", "DATETIME", "TIMESTAMP":
			if col.DatetimePrecision.Valid {
				precision = &col.DatetimePrecision.Int64
			}
		}
		// INFORMATION_SCHEMA exposes VECTOR(n)'s dimension in COLUMN_TYPE.
		// Convert it to its packed byte width for the common converter.
		if strings.EqualFold(col.DataType, "VECTOR") {
			if dimension, ok := parseTypeDimension(col.ColumnType, "VECTOR"); ok {
				packedLength := int64(dimension) * 4
				length = &packedLength
			}
		}
		if strings.EqualFold(col.DataType, "BIT") {
			if bitWidth, ok := parseTypeDimension(col.ColumnType, "BIT"); ok {
				width := int64(bitWidth)
				length = &width
			}
		}

		colType := sqlwrapper.ColumnType{
			Name:             col.ColumnName,
			DatabaseTypeName: dbTypeName,
			Nullable:         col.IsNullable == "YES",
			Length:           length,
			Precision:        precision,
			Scale:            scale,
		}

		arrowType, nullable, metadata, err := typeConverter.ConvertRawColumnType(colType)
		if err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to convert column type for %s", col.ColumnName)
		}

		metadataMap := metadata.ToMap()
		metadataMap[metaKeyMariaDBColumnType] = col.ColumnType
		if col.CharacterSetName.Valid {
			metadataMap["mariadb.character_set"] = col.CharacterSetName.String
		}
		if col.CollationName.Valid {
			metadataMap["mariadb.collation"] = col.CollationName.String
		}
		switch strings.ToUpper(col.DataType) {
		case "ENUM", "SET":
			values, parseErr := parseMariaDBTypeValues(col.ColumnType)
			if parseErr != nil {
				return nil, c.ErrorHelper.WrapIO(parseErr, "failed to parse %s declaration for %s", col.DataType, col.ColumnName)
			}
			encoded, marshalErr := json.Marshal(values)
			if marshalErr != nil {
				return nil, c.ErrorHelper.WrapIO(marshalErr, "failed to encode %s values for %s", col.DataType, col.ColumnName)
			}
			key := metaKeyMariaDBEnumValues
			if strings.EqualFold(col.DataType, "SET") {
				key = metaKeyMariaDBSetValues
			}
			metadataMap[key] = string(encoded)
		}

		fields[i] = arrow.Field{
			Name:     col.ColumnName,
			Type:     arrowType,
			Nullable: nullable,
			Metadata: arrow.MetadataFrom(metadataMap),
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func parseTypeDimension(columnType, expectedType string) (int, bool) {
	text := strings.TrimSpace(columnType)
	prefix := expectedType + "("
	if len(text) < len(prefix)+1 || !strings.EqualFold(text[:len(prefix)], prefix) || text[len(text)-1] != ')' {
		return 0, false
	}
	dimension, err := strconv.Atoi(strings.TrimSpace(text[len(prefix) : len(text)-1]))
	return dimension, err == nil && dimension > 0
}

// parseMariaDBTypeValues parses ENUM('a','b') and SET('a','b') declarations,
// including MariaDB's doubled-quote and backslash escapes.
func parseMariaDBTypeValues(columnType string) ([]string, error) {
	open := strings.IndexByte(columnType, '(')
	close := strings.LastIndexByte(columnType, ')')
	if open < 0 || close <= open {
		return nil, fmt.Errorf("invalid declaration %q", columnType)
	}
	input := columnType[open+1 : close]
	values := make([]string, 0)
	for pos := 0; pos < len(input); {
		for pos < len(input) && (input[pos] == ' ' || input[pos] == ',') {
			pos++
		}
		if pos == len(input) {
			break
		}
		if input[pos] != '\'' {
			return nil, fmt.Errorf("invalid declaration %q at byte %d", columnType, pos)
		}
		pos++
		var value strings.Builder
		closed := false
		for pos < len(input) {
			ch := input[pos]
			pos++
			if ch == '\\' && pos < len(input) {
				value.WriteByte(input[pos])
				pos++
				continue
			}
			if ch == '\'' {
				if pos < len(input) && input[pos] == '\'' {
					value.WriteByte('\'')
					pos++
					continue
				}
				closed = true
				break
			}
			value.WriteByte(ch)
		}
		if !closed {
			return nil, fmt.Errorf("unterminated value in declaration %q", columnType)
		}
		values = append(values, value.String())
		for pos < len(input) && input[pos] == ' ' {
			pos++
		}
		if pos < len(input) && input[pos] != ',' {
			return nil, fmt.Errorf("invalid declaration %q at byte %d", columnType, pos)
		}
	}
	return values, nil
}

// ListTableTypes implements driverbase.TableTypeLister interface
func (c *mariadbConnectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// MariaDB supports these standard table types
	return []string{
		"BASE TABLE",  // Regular tables
		"VIEW",        // Views
		"SYSTEM VIEW", // System/information schema views
	}, nil
}

// QuoteIdentifier implements BulkIngester
func (c *mariadbConnectionImpl) QuoteIdentifiers(parts []string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = quoteIdentifier(p)
	}
	return strings.Join(quoted, ".")
}

// GetPlaceholder implements BulkIngester
func (c *mariadbConnectionImpl) GetPlaceholder(field *arrow.Field, index int) string {
	if logicalType, ok := field.Metadata.GetValue(metaKeyLogicalArrowType); ok && logicalType == "geoarrow.wkb" {
		return "ST_GeomFromWKB(?)"
	}
	return "?"
}

// Ensure mariadbConnectionImpl implements BulkIngester
var _ sqlwrapper.BulkIngester = (*mariadbConnectionImpl)(nil)

// ExecuteBulkIngest performs MariaDB bulk ingest using batched INSERT statements.
func (c *mariadbConnectionImpl) ExecuteBulkIngest(ctx context.Context, stmt sqlwrapper.StatementImpl, conn *sqlwrapper.LoggingConn, options *driverbase.BulkIngestOptions, stream array.RecordReader) (rowCount int64, err error) {
	schema := stream.Schema()

	// Validate MariaDB-specific options before touching the database.
	if options.MaxQuerySizeBytes > 0 {
		return -1, c.ErrorHelper.InvalidArgument(
			"MariaDB driver does not support '%s'. "+
				"Use '%s' instead to control the number of rows per INSERT statement.",
			driverbase.OptionKeyIngestMaxQuerySizeBytes,
			driverbase.OptionKeyIngestBatchSize)
	}

	// Guard against division by zero and schemas that exceed MariaDB's 65,535
	// placeholder limit before any database interaction.
	numCols := len(schema.Fields())
	if numCols == 0 {
		return -1, c.ErrorHelper.InvalidArgument("bulk ingest schema must have at least one column")
	}
	if numCols > MariaDBMaxPlaceholders {
		return -1, c.ErrorHelper.InvalidArgument(
			"bulk ingest schema has %d columns, exceeding the MariaDB placeholder limit of %d",
			numCols, MariaDBMaxPlaceholders)
	}

	if err := c.createTableIfNeeded(ctx, conn, options.TableName, schema, options); err != nil {
		return -1, c.ErrorHelper.WrapIO(err, "failed to create table")
	}

	// Set MariaDB-specific default batch size if user hasn't overridden,
	// capping to stay within MariaDB's 65,535 placeholder limit.
	maxBatchSize := MariaDBMaxPlaceholders / numCols
	if options.IngestBatchSize == 0 {
		options.IngestBatchSize = min(MariaDBDefaultIngestBatchSize, maxBatchSize)
	} else if options.IngestBatchSize > maxBatchSize {
		options.IngestBatchSize = maxBatchSize
	}

	return sqlwrapper.ExecuteBatchedBulkIngest(
		ctx, stmt, conn, options, stream,
		stmt.MakeTypeConverter("MariaDB"), c, &c.Base().ErrorHelper,
	)
}

// createTableIfNeeded creates the table based on the ingest mode
func (c *mariadbConnectionImpl) createTableIfNeeded(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, options *driverbase.BulkIngestOptions) error {
	switch options.Mode {
	case adbc.OptionValueIngestModeCreate:
		// Create the table (fail if exists)
		return c.createTable(ctx, conn, tableName, schema, false, options.Temporary)
	case adbc.OptionValueIngestModeCreateAppend:
		// Create the table if it doesn't exist
		return c.createTable(ctx, conn, tableName, schema, true, options.Temporary)
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate the table
		if err := c.dropTable(ctx, conn, tableName, options.Temporary); err != nil {
			return err
		}
		return c.createTable(ctx, conn, tableName, schema, false, options.Temporary)
	case adbc.OptionValueIngestModeAppend:
		// Table should already exist, do nothing
		return nil
	default:
		return c.ErrorHelper.InvalidArgument("unsupported ingest mode: %s", options.Mode)
	}
}

// createTable creates a MariaDB table from Arrow schema
func (c *mariadbConnectionImpl) createTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, schema *arrow.Schema, ifNotExists bool, temporary bool) error {
	var queryBuilder strings.Builder
	if temporary {
		queryBuilder.WriteString("CREATE TEMPORARY TABLE ")
	} else {
		queryBuilder.WriteString("CREATE TABLE ")
	}
	if ifNotExists {
		queryBuilder.WriteString("IF NOT EXISTS ")
	}
	queryBuilder.WriteString(quoteIdentifier(tableName))
	queryBuilder.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		queryBuilder.WriteString(quoteIdentifier(field.Name))
		queryBuilder.WriteString(" ")

		// Convert Arrow type to MariaDB type
		mariadbType := c.arrowToMariaDBType(field.Type, field.Nullable)
		queryBuilder.WriteString(mariadbType)
	}

	queryBuilder.WriteString(")")

	_, err := conn.ExecContext(ctx, queryBuilder.String())
	return err
}

// dropTable drops a MariaDB table
func (c *mariadbConnectionImpl) dropTable(ctx context.Context, conn *sqlwrapper.LoggingConn, tableName string, temporary bool) error {
	keyword := "TABLE"
	if temporary {
		keyword = "TEMPORARY TABLE"
	}
	dropSQL := fmt.Sprintf("DROP %s IF EXISTS %s", keyword, quoteIdentifier(tableName))
	_, err := conn.ExecContext(ctx, dropSQL)
	return err
}

// arrowToMariaDBType converts Arrow data type to MariaDB column type
func (c *mariadbConnectionImpl) arrowToMariaDBType(arrowType arrow.DataType, nullable bool) string {
	var mariadbType string

	switch arrowType := arrowType.(type) {
	case *arrow.BooleanType:
		mariadbType = "BOOLEAN"
	case *arrow.Int8Type:
		mariadbType = "TINYINT"
	case *arrow.Int16Type:
		mariadbType = "SMALLINT"
	case *arrow.Int32Type:
		mariadbType = "INT"
	case *arrow.Int64Type:
		mariadbType = "BIGINT"
	case *arrow.Uint8Type:
		mariadbType = "TINYINT UNSIGNED"
	case *arrow.Uint16Type:
		mariadbType = "SMALLINT UNSIGNED"
	case *arrow.Uint32Type:
		mariadbType = "INT UNSIGNED"
	case *arrow.Uint64Type:
		mariadbType = "BIGINT UNSIGNED"
	case *arrow.Float32Type:
		mariadbType = "FLOAT"
	case *arrow.Float64Type:
		mariadbType = "DOUBLE"
	case *arrow.StringType:
		mariadbType = "TEXT"
	case *arrow.BinaryType, *arrow.FixedSizeBinaryType, *arrow.BinaryViewType:
		mariadbType = "BLOB"
	case *arrow.LargeBinaryType:
		mariadbType = "LONGBLOB"
	case *arrow.Date32Type:
		mariadbType = "DATE"
	case *arrow.TimestampType:

		// Determine precision based on Arrow timestamp unit
		var precision string
		switch arrowType.Unit {
		case arrow.Second:
			precision = ""
		case arrow.Millisecond:
			precision = "(3)"
		case arrow.Microsecond:
			precision = "(6)"
		case arrow.Nanosecond:
			precision = "(6)" // MariaDB max is 6 digits
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Arrow timestamp unit: %v", arrowType.Unit))
		}

		// Use DATETIME for timezone-naive timestamps, TIMESTAMP for timezone-aware
		if arrowType.TimeZone != "" {
			// Timezone-aware (timestamptz) -> TIMESTAMP
			mariadbType = "TIMESTAMP" + precision
		} else {
			// Timezone-naive (timestamp) -> DATETIME
			mariadbType = "DATETIME" + precision
		}
	case *arrow.Time32Type:
		// Determine precision based on Arrow time unit
		switch arrowType.Unit {
		case arrow.Second:
			mariadbType = "TIME"
		case arrow.Millisecond:
			mariadbType = "TIME(3)"
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Time32 unit: %v", arrowType.Unit))
		}

	case *arrow.Time64Type:
		// Determine precision based on Arrow time unit
		switch arrowType.Unit {
		case arrow.Microsecond:
			mariadbType = "TIME(6)"
		case arrow.Nanosecond:
			mariadbType = "TIME(6)" // MariaDB max is 6 digits
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Time64 unit: %v", arrowType.Unit))
		}
	case *arrow.DurationType:
		// MariaDB TIME is a signed duration, not merely a time of day.
		switch arrowType.Unit {
		case arrow.Second:
			mariadbType = "TIME"
		case arrow.Millisecond:
			mariadbType = "TIME(3)"
		default:
			mariadbType = "TIME(6)"
		}
	case arrow.DecimalType:
		mariadbType = fmt.Sprintf("DECIMAL(%d,%d)", arrowType.GetPrecision(), arrowType.GetScale())
	default:
		// Default to TEXT for unknown types
		mariadbType = "TEXT"
	}

	if nullable {
		mariadbType += " NULL"
	} else {
		mariadbType += " NOT NULL"
	}

	return mariadbType
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
