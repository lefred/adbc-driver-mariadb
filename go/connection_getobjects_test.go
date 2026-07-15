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
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildColumnInfoExtendedMetadata(t *testing.T) {
	info := buildColumnInfo(
		"decimal", "decimal(65,30)", "amount", "NO", 4,
		sql.NullString{String: "exact amount", Valid: true},
		sql.NullString{String: "0.000000000000000000000000000000", Valid: true},
		sql.NullInt64{}, sql.NullInt64{},
		sql.NullInt64{Int64: 65, Valid: true},
		sql.NullInt64{Int64: 30, Valid: true}, sql.NullInt64{},
		sql.NullString{}, sql.NullString{},
	)

	require.Equal(t, "decimal", *info.XdbcTypeName)
	require.Equal(t, int32(65), *info.XdbcColumnSize)
	require.Equal(t, int16(30), *info.XdbcDecimalDigits)
	require.Equal(t, int16(10), *info.XdbcNumPrecRadix)
	require.Equal(t, "0.000000000000000000000000000000", *info.XdbcColumnDef)
	require.Equal(t, "exact amount", *info.Remarks)
	require.False(t, *info.XdbcIsAutoincrement)
	require.False(t, *info.XdbcIsGeneratedcolumn)
}

func TestBuildColumnInfoGeneratedInvisibleAutoincrement(t *testing.T) {
	generated := buildColumnInfo(
		"int", "int(11)", "computed", "YES", 2,
		sql.NullString{String: "derived", Valid: true}, sql.NullString{},
		sql.NullInt64{}, sql.NullInt64{},
		sql.NullInt64{Int64: 10, Valid: true}, sql.NullInt64{Int64: 0, Valid: true},
		sql.NullInt64{},
		sql.NullString{String: "VIRTUAL GENERATED INVISIBLE", Valid: true},
		sql.NullString{String: "`source` + 1", Valid: true},
	)
	require.True(t, *generated.XdbcIsGeneratedcolumn)
	require.False(t, *generated.XdbcIsAutoincrement)
	require.Equal(t, "derived [MariaDB: INVISIBLE]", *generated.Remarks)

	autoincrement := buildColumnInfo(
		"bigint", "bigint(20) unsigned", "id", "NO", 1,
		sql.NullString{}, sql.NullString{}, sql.NullInt64{}, sql.NullInt64{},
		sql.NullInt64{Int64: 20, Valid: true}, sql.NullInt64{Int64: 0, Valid: true},
		sql.NullInt64{}, sql.NullString{String: "auto_increment", Valid: true}, sql.NullString{},
	)
	require.Equal(t, "bigint UNSIGNED", *autoincrement.XdbcTypeName)
	require.True(t, *autoincrement.XdbcIsAutoincrement)
	require.False(t, *autoincrement.XdbcIsGeneratedcolumn)
}

func TestBuildColumnInfoCharacterLengths(t *testing.T) {
	info := buildColumnInfo(
		"varchar", "varchar(20)", "name", "YES", 1,
		sql.NullString{}, sql.NullString{},
		sql.NullInt64{Int64: 20, Valid: true},
		sql.NullInt64{Int64: 80, Valid: true},
		sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{},
		sql.NullString{}, sql.NullString{},
	)
	require.Equal(t, int32(20), *info.XdbcColumnSize)
	require.Equal(t, int32(80), *info.XdbcCharOctetLength)
	require.Nil(t, info.XdbcDecimalDigits)
}
