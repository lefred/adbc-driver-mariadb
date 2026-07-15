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
	"errors"
	"math"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// GetObjects implements adbc.Connection by running a single query on the
// session connection (c.Conn) so that session-scoped objects like temporary
// tables are visible in the results.
//
// The query joins SCHEMATA, a synthetic schema subquery, TABLES, and COLUMNS.
// Levels beyond the requested depth are disabled with AND 1=0 in the join
// condition. All filters (catalog, schema, table, column, table type) are
// applied in SQL.
func (c *mariadbConnectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	if err := c.ClearPending(); err != nil {
		return nil, err
	}

	includeSchemas := depth != adbc.ObjectDepthCatalogs
	includeTables := depth == adbc.ObjectDepthTables || depth == adbc.ObjectDepthColumns
	includeColumns := depth == adbc.ObjectDepthColumns

	var queryBuilder strings.Builder
	args := []any{}

	queryBuilder.WriteString(`
		SELECT
			s.SCHEMA_NAME AS CATALOG_NAME,
			sch.DB_SCHEMA_NAME,
			t.TABLE_NAME,
			t.TABLE_TYPE,
			c.ORDINAL_POSITION,
			c.COLUMN_NAME,
			c.COLUMN_COMMENT,
			c.DATA_TYPE,
			c.COLUMN_TYPE,
			c.IS_NULLABLE,
			c.COLUMN_DEFAULT,
			c.CHARACTER_MAXIMUM_LENGTH,
			c.CHARACTER_OCTET_LENGTH,
			c.NUMERIC_PRECISION,
			c.NUMERIC_SCALE,
			c.DATETIME_PRECISION,
			c.EXTRA,
			c.GENERATION_EXPRESSION
		FROM INFORMATION_SCHEMA.SCHEMATA s`)

	// MariaDB has no real schema concept. Model it as a single empty-string
	// schema via a LEFT JOIN with a synthetic row. Schema-oriented ADBC clients
	// can assign their own external name (DuckDB maps this to "main"). The
	// schema filter is applied via LIKE on this column. AND 1=0 disables the
	// join when depth is catalogs-only, producing NULL for DB_SCHEMA_NAME.
	queryBuilder.WriteString(`
		LEFT JOIN (SELECT '' AS DB_SCHEMA_NAME) sch
			ON 1=1`)

	if !includeSchemas {
		queryBuilder.WriteString(` AND 1=0`)
	} else if dbSchema != nil {
		queryBuilder.WriteString(` AND sch.DB_SCHEMA_NAME LIKE ?`)
		args = append(args, *dbSchema)
	}

	queryBuilder.WriteString(`
		LEFT JOIN INFORMATION_SCHEMA.TABLES t
			ON s.SCHEMA_NAME = t.TABLE_SCHEMA
			AND sch.DB_SCHEMA_NAME IS NOT NULL`)

	if !includeTables {
		queryBuilder.WriteString(` AND 1=0`)
	} else {
		if tableName != nil {
			queryBuilder.WriteString(` AND t.TABLE_NAME LIKE ?`)
			args = append(args, *tableName)
		}
		if len(tableType) > 0 {
			queryBuilder.WriteString(` AND t.TABLE_TYPE IN (` + placeholders(len(tableType)) + `)`)
			for _, tt := range tableType {
				args = append(args, tt)
			}
		}
	}

	queryBuilder.WriteString(`
		LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
			ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
			AND t.TABLE_NAME = c.TABLE_NAME`)

	if !includeColumns {
		queryBuilder.WriteString(` AND 1=0`)
	} else if columnName != nil {
		queryBuilder.WriteString(` AND c.COLUMN_NAME LIKE ?`)
		args = append(args, *columnName)
	}

	if catalog != nil {
		queryBuilder.WriteString(` WHERE s.SCHEMA_NAME LIKE ?`)
		args = append(args, *catalog)
	}

	queryBuilder.WriteString(` ORDER BY s.SCHEMA_NAME, t.TABLE_NAME, c.ORDINAL_POSITION`)

	rows, err := c.Conn.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query objects")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	// Group rows into the GetObjectsInfo hierarchy.
	var infos []driverbase.GetObjectsInfo
	var currentInfo *driverbase.GetObjectsInfo
	var currentTable *driverbase.TableInfo

	for rows.Next() {
		var (
			catalogName          string
			schemaName           sql.NullString
			tblName              sql.NullString
			tblType              sql.NullString
			ordinalPosition      sql.NullInt32
			colName              sql.NullString
			colComment           sql.NullString
			dataType             sql.NullString
			colType              sql.NullString
			isNullable           sql.NullString
			colDefault           sql.NullString
			charMaxLength        sql.NullInt64
			charOctetLength      sql.NullInt64
			numericPrecision     sql.NullInt64
			numericScale         sql.NullInt64
			datetimePrecision    sql.NullInt64
			extra                sql.NullString
			generationExpression sql.NullString
		)

		if err := rows.Scan(
			&catalogName, &schemaName, &tblName, &tblType,
			&ordinalPosition, &colName, &colComment,
			&dataType, &colType, &isNullable, &colDefault,
			&charMaxLength, &charOctetLength, &numericPrecision,
			&numericScale, &datetimePrecision, &extra, &generationExpression,
		); err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan objects row")
		}

		// New catalog?
		if currentInfo == nil || *currentInfo.CatalogName != catalogName {
			info := driverbase.GetObjectsInfo{CatalogName: driverbase.Nullable(catalogName)}
			if schemaName.Valid {
				schemaInfo := driverbase.DBSchemaInfo{DbSchemaName: driverbase.Nullable(schemaName.String)}
				if includeTables {
					schemaInfo.DbSchemaTables = []driverbase.TableInfo{}
				}
				info.CatalogDbSchemas = []driverbase.DBSchemaInfo{schemaInfo}
			} else if includeSchemas {
				info.CatalogDbSchemas = []driverbase.DBSchemaInfo{}
			}
			infos = append(infos, info)
			currentInfo = &infos[len(infos)-1]
			currentTable = nil
		}

		if !tblName.Valid {
			continue
		}

		// New table?
		tables := &currentInfo.CatalogDbSchemas[0].DbSchemaTables
		if currentTable == nil || currentTable.TableName != tblName.String {
			tableInfo := driverbase.TableInfo{
				TableName:        tblName.String,
				TableType:        tblType.String,
				TableConstraints: []driverbase.ConstraintInfo{},
			}
			if includeColumns {
				tableInfo.TableColumns = []driverbase.ColumnInfo{}
			}
			*tables = append(*tables, tableInfo)
			currentTable = &(*tables)[len(*tables)-1]
		}

		if !colName.Valid {
			continue
		}

		currentTable.TableColumns = append(currentTable.TableColumns,
			buildColumnInfo(dataType.String, colType.String, colName.String, isNullable.String,
				ordinalPosition.Int32, colComment, colDefault, charMaxLength, charOctetLength,
				numericPrecision, numericScale, datetimePrecision, extra, generationExpression))
	}

	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "error during objects iteration")
	}
	if err := rows.Close(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to close objects rows")
	}

	if includeTables {
		if err := c.addTableConstraints(ctx, infos, catalog, tableName, tableType); err != nil {
			return nil, err
		}
	}

	return buildResult(c, infos)
}

// addTableConstraints populates the standard ADBC table_constraints list.
// MariaDB databases are ADBC catalogs and have an empty ADBC schema.
func (c *mariadbConnectionImpl) addTableConstraints(ctx context.Context, infos []driverbase.GetObjectsInfo,
	catalog, tableName *string, tableType []string) error {
	tables := make(map[string]*driverbase.TableInfo)
	for infoIdx := range infos {
		if infos[infoIdx].CatalogName == nil {
			continue
		}
		for schemaIdx := range infos[infoIdx].CatalogDbSchemas {
			for tableIdx := range infos[infoIdx].CatalogDbSchemas[schemaIdx].DbSchemaTables {
				table := &infos[infoIdx].CatalogDbSchemas[schemaIdx].DbSchemaTables[tableIdx]
				tables[*infos[infoIdx].CatalogName+"\x00"+table.TableName] = table
			}
		}
	}
	if len(tables) == 0 {
		return nil
	}

	var query strings.Builder
	args := []any{}
	query.WriteString(`SELECT
		tc.CONSTRAINT_SCHEMA,
		tc.TABLE_NAME,
		tc.CONSTRAINT_NAME,
		tc.CONSTRAINT_TYPE,
		kcu.ORDINAL_POSITION,
		kcu.COLUMN_NAME,
		kcu.REFERENCED_TABLE_SCHEMA,
		kcu.REFERENCED_TABLE_NAME,
		kcu.REFERENCED_COLUMN_NAME
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
	JOIN INFORMATION_SCHEMA.TABLES t
	  ON t.TABLE_SCHEMA = tc.TABLE_SCHEMA AND t.TABLE_NAME = tc.TABLE_NAME
	LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
	  ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
	 AND kcu.TABLE_NAME = tc.TABLE_NAME
	 AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
	WHERE 1=1`)
	if catalog != nil {
		query.WriteString(` AND tc.CONSTRAINT_SCHEMA LIKE ?`)
		args = append(args, *catalog)
	}
	if tableName != nil {
		query.WriteString(` AND tc.TABLE_NAME LIKE ?`)
		args = append(args, *tableName)
	}
	if len(tableType) > 0 {
		query.WriteString(` AND t.TABLE_TYPE IN (` + placeholders(len(tableType)) + `)`)
		for _, value := range tableType {
			args = append(args, value)
		}
	}
	query.WriteString(` ORDER BY tc.CONSTRAINT_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`)

	rows, err := c.Conn.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return c.ErrorHelper.WrapIO(err, "failed to query table constraints")
	}
	defer rows.Close()

	var currentTable *driverbase.TableInfo
	var currentConstraint *driverbase.ConstraintInfo
	var currentKey string
	var currentName string
	for rows.Next() {
		var (
			catalogName     string
			tblName         string
			constraintName  string
			constraintType  string
			ordinalPosition sql.NullInt64
			columnName      sql.NullString
			foreignCatalog  sql.NullString
			foreignTable    sql.NullString
			foreignColumn   sql.NullString
		)
		if err := rows.Scan(&catalogName, &tblName, &constraintName, &constraintType,
			&ordinalPosition, &columnName, &foreignCatalog, &foreignTable, &foreignColumn); err != nil {
			return c.ErrorHelper.WrapIO(err, "failed to scan table constraint")
		}
		tableKey := catalogName + "\x00" + tblName
		if tableKey != currentKey {
			currentTable = tables[tableKey]
			currentConstraint = nil
			currentKey = tableKey
			currentName = ""
		}
		if currentTable == nil {
			continue
		}
		if currentConstraint == nil || currentName != constraintName {
			info := driverbase.ConstraintInfo{
				ConstraintName:        driverbase.Nullable(constraintName),
				ConstraintType:        constraintType,
				ConstraintColumnNames: driverbase.RequiredList([]string{}),
			}
			currentTable.TableConstraints = append(currentTable.TableConstraints, info)
			currentConstraint = &currentTable.TableConstraints[len(currentTable.TableConstraints)-1]
			currentName = constraintName
		}
		if columnName.Valid {
			currentConstraint.ConstraintColumnNames = driverbase.RequiredList(
				append([]string(currentConstraint.ConstraintColumnNames), columnName.String))
		}
		if foreignTable.Valid && foreignColumn.Valid {
			usage := driverbase.ConstraintColumnUsage{
				ForeignKeyDbSchema: driverbase.Nullable(""),
				ForeignKeyTable:    foreignTable.String,
				ForeignKeyColumn:   foreignColumn.String,
			}
			if foreignCatalog.Valid {
				usage.ForeignKeyCatalog = driverbase.Nullable(foreignCatalog.String)
			}
			currentConstraint.ConstraintColumnUsage = append(currentConstraint.ConstraintColumnUsage, usage)
		}
	}
	if err := rows.Err(); err != nil {
		return c.ErrorHelper.WrapIO(err, "error during table constraint iteration")
	}
	return nil
}

// buildColumnInfo constructs a ColumnInfo from raw MariaDB column metadata.
func buildColumnInfo(dataType, columnType, columnName, isNullable string, ordinalPosition int32,
	columnComment, columnDefault sql.NullString, charMaxLength, charOctetLength,
	numericPrecision, numericScale, datetimePrecision sql.NullInt64,
	extra, generationExpression sql.NullString) driverbase.ColumnInfo {
	var radix sql.NullInt16
	var nullable sql.NullInt16

	// Build the full type name including UNSIGNED if applicable
	// Only check integer types to avoid false positives with enum/set value lists
	xdbcTypeName := dataType
	switch strings.ToUpper(dataType) {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT":
		if strings.Contains(strings.ToUpper(columnType), "UNSIGNED") {
			xdbcTypeName = dataType + " UNSIGNED"
		}
	}

	// Set numeric precision radix (MariaDB doesn't store this directly)
	switch strings.ToUpper(dataType) {
	case "BIT":
		radix = sql.NullInt16{Int16: 2, Valid: true}
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
		"DECIMAL", "DEC", "NUMERIC", "FIXED",
		"FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL",
		"YEAR":
		radix = sql.NullInt16{Int16: 10, Valid: true}
	default:
		radix = sql.NullInt16{Valid: false}
	}

	// Set nullable information
	switch isNullable {
	case "YES":
		nullable = sql.NullInt16{Int16: int16(driverbase.XdbcColumnNullable), Valid: true}
	case "NO":
		nullable = sql.NullInt16{Int16: int16(driverbase.XdbcColumnNoNulls), Valid: true}
	}

	remarks := columnComment
	if extra.Valid && strings.Contains(strings.ToUpper(extra.String), "INVISIBLE") {
		const invisibleMarker = "[MariaDB: INVISIBLE]"
		if remarks.Valid && remarks.String != "" {
			remarks.String += " " + invisibleMarker
		} else {
			remarks = sql.NullString{String: invisibleMarker, Valid: true}
		}
	}

	var columnSize sql.NullInt32
	for _, candidate := range []sql.NullInt64{charMaxLength, numericPrecision, datetimePrecision} {
		if candidate.Valid {
			columnSize = sql.NullInt32{Int32: clampInt32(candidate.Int64), Valid: true}
			break
		}
	}
	var decimalDigits sql.NullInt16
	if numericScale.Valid {
		decimalDigits = sql.NullInt16{Int16: clampInt16(numericScale.Int64), Valid: true}
	} else if datetimePrecision.Valid {
		decimalDigits = sql.NullInt16{Int16: clampInt16(datetimePrecision.Int64), Valid: true}
	}
	var octetLength sql.NullInt32
	if charOctetLength.Valid {
		octetLength = sql.NullInt32{Int32: clampInt32(charOctetLength.Int64), Valid: true}
	}
	isAutoincrement := extra.Valid && strings.Contains(strings.ToLower(extra.String), "auto_increment")
	isGenerated := (extra.Valid && strings.Contains(strings.ToUpper(extra.String), "GENERATED")) ||
		(generationExpression.Valid && generationExpression.String != "")

	return driverbase.ColumnInfo{
		ColumnName:            columnName,
		OrdinalPosition:       &ordinalPosition,
		Remarks:               driverbase.NullStringToPtr(remarks),
		XdbcTypeName:          &xdbcTypeName,
		XdbcColumnSize:        driverbase.NullInt32ToPtr(columnSize),
		XdbcDecimalDigits:     driverbase.NullInt16ToPtr(decimalDigits),
		XdbcNumPrecRadix:      driverbase.NullInt16ToPtr(radix),
		XdbcNullable:          driverbase.NullInt16ToPtr(nullable),
		XdbcIsNullable:        &isNullable,
		XdbcColumnDef:         driverbase.NullStringToPtr(columnDefault),
		XdbcCharOctetLength:   driverbase.NullInt32ToPtr(octetLength),
		XdbcIsAutoincrement:   &isAutoincrement,
		XdbcIsGeneratedcolumn: &isGenerated,
	}
}

func clampInt32(value int64) int32 {
	return int32(max(int64(math.MinInt32), min(int64(math.MaxInt32), value)))
}

func clampInt16(value int64) int16 {
	return int16(max(int64(math.MinInt16), min(int64(math.MaxInt16), value)))
}

// placeholders returns a comma-separated string of n question marks.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

// buildResult feeds GetObjectsInfo entries into BuildGetObjectsRecordReader.
func buildResult(c *mariadbConnectionImpl, infos []driverbase.GetObjectsInfo) (array.RecordReader, error) {
	ch := make(chan driverbase.GetObjectsInfo, len(infos))
	for _, info := range infos {
		ch <- info
	}
	close(ch)

	errCh := make(chan error, 1)
	close(errCh)

	return driverbase.BuildGetObjectsRecordReader(c.Alloc, ch, errCh)
}
