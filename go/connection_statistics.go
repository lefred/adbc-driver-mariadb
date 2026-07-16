// Copyright (c) 2026 ADBC Drivers Contributors
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
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	statisticDataLengthKey   int16 = 1024
	statisticIndexLengthKey  int16 = 1025
	statisticAvgRowLengthKey int16 = 1026

	statisticDataLengthName   = "mariadb.statistic.data_length"
	statisticIndexLengthName  = "mariadb.statistic.index_length"
	statisticAvgRowLengthName = "mariadb.statistic.avg_row_length"
)

var mariadbStatisticNames = []driverbase.StatisticNameKey{
	{Name: statisticDataLengthName, Key: statisticDataLengthKey},
	{Name: statisticIndexLengthName, Key: statisticIndexLengthKey},
	{Name: statisticAvgRowLengthName, Key: statisticAvgRowLengthKey},
}

// GetStatisticNames returns the MariaDB-specific statistics emitted by
// GetStatistics. Standard ADBC statistics are intentionally not included.
func (c *mariadbConnectionImpl) GetStatisticNames(context.Context) (array.RecordReader, error) {
	return driverbase.BuildGetStatisticNamesReader(c.Alloc, mariadbStatisticNames)
}

// GetStatistics returns inexpensive, best-effort statistics from MariaDB's
// information_schema metadata. InnoDB table row counts and index cardinalities
// are estimates, so metadata values are marked approximate even when the caller
// requests exact statistics.
func (c *mariadbConnectionImpl) GetStatistics(ctx context.Context, catalog, dbSchema, tableName *string, approximate bool) (array.RecordReader, error) {
	if err := c.ClearPending(); err != nil {
		return nil, err
	}

	// MariaDB databases are ADBC catalogs and have no schema level.
	if dbSchema != nil && *dbSchema != "" {
		return driverbase.EmptyGetStatisticsReader()
	}

	where, args := statisticsFilter(catalog, tableName)
	query := `
		SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS, AVG_ROW_LENGTH,
		       DATA_LENGTH, INDEX_LENGTH
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE'` + where + `
		ORDER BY TABLE_SCHEMA, TABLE_NAME`

	rows, err := c.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query table statistics")
	}

	catalogOrder := []string{}
	schemaOrder := make(map[string][]string)
	statsByCatalog := make(map[string]map[string][]driverbase.Statistic)
	knownTables := make(map[string]struct{})

	for rows.Next() {
		var catalogName, table string
		var rowCount, avgRowLength, dataLength, indexLength sql.Null[uint64]
		if err := rows.Scan(&catalogName, &table, &rowCount, &avgRowLength, &dataLength, &indexLength); err != nil {
			_ = rows.Close()
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan table statistics")
		}

		if _, exists := statsByCatalog[catalogName]; !exists {
			catalogOrder = append(catalogOrder, catalogName)
			schemaOrder[catalogName] = []string{""}
			statsByCatalog[catalogName] = map[string][]driverbase.Statistic{"": {}}
		}
		knownTables[catalogName+"\x00"+table] = struct{}{}
		stats := statsByCatalog[catalogName][""]
		if rowCount.Valid {
			stats = append(stats, driverbase.NewUint64Stat(table, nil, adbc.StatisticRowCountKey, rowCount.V, true))
		}
		if dataLength.Valid {
			stats = append(stats, driverbase.NewUint64Stat(table, nil, statisticDataLengthKey, dataLength.V, true))
		}
		if indexLength.Valid {
			stats = append(stats, driverbase.NewUint64Stat(table, nil, statisticIndexLengthKey, indexLength.V, true))
		}
		if avgRowLength.Valid {
			stats = append(stats, driverbase.NewUint64Stat(table, nil, statisticAvgRowLengthKey, avgRowLength.V, true))
		}
		statsByCatalog[catalogName][""] = stats
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to read table statistics")
	}

	// STATISTICS.CARDINALITY is maintained per index prefix. Taking the maximum
	// for a column provides the best available metadata estimate when a column
	// appears in more than one index.
	indexQuery := `
		SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, MAX(CARDINALITY)
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE CARDINALITY IS NOT NULL` + where + `
		GROUP BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
		ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME`
	rows, err = c.Conn.QueryContext(ctx, indexQuery, args...)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query index statistics")
	}
	for rows.Next() {
		var catalogName, table, column string
		var cardinality sql.Null[uint64]
		if err := rows.Scan(&catalogName, &table, &column, &cardinality); err != nil {
			_ = rows.Close()
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan index statistics")
		}
		if !cardinality.Valid {
			continue
		}
		if _, exists := knownTables[catalogName+"\x00"+table]; !exists {
			continue
		}
		statsByCatalog[catalogName][""] = append(statsByCatalog[catalogName][""],
			driverbase.NewUint64Stat(table, &column, adbc.StatisticDistinctCountKey, cardinality.V, true))
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to read index statistics")
	}

	if len(catalogOrder) == 0 {
		return driverbase.EmptyGetStatisticsReader()
	}
	return driverbase.BuildGetStatisticsReader(c.Alloc, catalogOrder, schemaOrder, statsByCatalog)
}

func statisticsFilter(catalog, tableName *string) (string, []any) {
	var where strings.Builder
	var args []any
	if catalog != nil {
		where.WriteString(" AND TABLE_SCHEMA LIKE ?")
		args = append(args, *catalog)
	}
	if tableName != nil {
		where.WriteString(" AND TABLE_NAME LIKE ?")
		args = append(args, *tableName)
	}
	return where.String(), args
}

var _ adbc.ConnectionGetStatistics = (*mariadbConnectionImpl)(nil)
