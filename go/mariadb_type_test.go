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
	"encoding/binary"
	"math"
	"testing"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func int64ptr(value int64) *int64 { return &value }

func TestMariaDBUUIDConversion(t *testing.T) {
	converter := makeTypeConverter(zeroDatetimeBehaviorError)
	typeInfo := sqlwrapper.ColumnType{
		Name:             "id",
		DatabaseTypeName: "UUID",
		Nullable:         false,
	}

	dataType, nullable, metadata, err := converter.ConvertRawColumnType(typeInfo)
	require.NoError(t, err)
	require.False(t, nullable)
	require.IsType(t, &extensions.UUIDType{}, dataType)

	field := arrow.Field{Name: "id", Type: dataType, Metadata: metadata}
	builder := extensions.NewUUIDBuilder(memory.DefaultAllocator)
	defer builder.Release()

	inserter, err := converter.CreateInserter(&field, builder)
	require.NoError(t, err)
	require.NoError(t, inserter.AppendValue([]byte("019f6239-5ae2-77e0-a8e5-ee4bd2bf9158")))

	values := builder.NewArray()
	defer values.Release()
	uuidValues := values.(*extensions.UUIDArray)
	require.Equal(t, "019f6239-5ae2-77e0-a8e5-ee4bd2bf9158", uuidValues.Value(0).String())

	bound, err := converter.ConvertArrowToGo(values, 0, &field)
	require.NoError(t, err)
	require.Equal(t, "019f6239-5ae2-77e0-a8e5-ee4bd2bf9158", bound)
}

func TestMariaDBNativeTypeMappings(t *testing.T) {
	converter := makeTypeConverter(zeroDatetimeBehaviorError)
	tests := []struct {
		name     string
		column   sqlwrapper.ColumnType
		expected arrow.DataType
		metaKey  string
		metaVal  string
	}{
		{"inet4", sqlwrapper.ColumnType{Name: "ip", DatabaseTypeName: "INET4"}, arrow.BinaryTypes.String, metaKeyMariaDBNativeType, "inet4"},
		{"inet6", sqlwrapper.ColumnType{Name: "ip", DatabaseTypeName: "INET6"}, arrow.BinaryTypes.String, metaKeyMariaDBNativeType, "inet6"},
		{"json", sqlwrapper.ColumnType{Name: "doc", DatabaseTypeName: "JSON"}, mustJSONType(t), metaKeyMariaDBNativeType, "json"},
		{"year", sqlwrapper.ColumnType{Name: "y", DatabaseTypeName: "YEAR"}, arrow.PrimitiveTypes.Int16, metaKeyMariaDBNativeType, "year"},
		{"uint64", sqlwrapper.ColumnType{Name: "n", DatabaseTypeName: "BIGINT UNSIGNED"}, arrow.PrimitiveTypes.Uint64, sqlwrapper.MetaKeyDatabaseTypeName, "BIGINT UNSIGNED"},
		{"decimal256_fallback", sqlwrapper.ColumnType{Name: "n", DatabaseTypeName: "DECIMAL", Precision: int64ptr(65), Scale: int64ptr(30)}, arrow.BinaryTypes.String, metaKeyArrowFallback, "string"},
		{"binary", sqlwrapper.ColumnType{Name: "b", DatabaseTypeName: "VARBINARY", Length: int64ptr(20)}, arrow.BinaryTypes.Binary, sqlwrapper.MetaKeyLength, "20"},
		{"text", sqlwrapper.ColumnType{Name: "s", DatabaseTypeName: "VARCHAR", Length: int64ptr(20)}, arrow.BinaryTypes.String, sqlwrapper.MetaKeyLength, "20"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual, _, metadata, err := converter.ConvertRawColumnType(tc.column)
			require.NoError(t, err)
			require.True(t, arrow.TypeEqual(tc.expected, actual), "expected %s, got %s", tc.expected, actual)
			value, ok := metadata.GetValue(tc.metaKey)
			require.True(t, ok)
			require.Equal(t, tc.metaVal, value)
		})
	}
}

func mustJSONType(t *testing.T) arrow.DataType {
	t.Helper()
	type_, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	require.NoError(t, err)
	return type_
}

func TestMariaDBVectorConversion(t *testing.T) {
	converter := makeTypeConverter(zeroDatetimeBehaviorError)
	typeInfo := sqlwrapper.ColumnType{
		Name: "embedding", DatabaseTypeName: "VECTOR", Length: int64ptr(12),
	}

	dataType, _, metadata, err := converter.ConvertRawColumnType(typeInfo)
	require.NoError(t, err)
	require.True(t, arrow.TypeEqual(arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32), dataType))
	dimension, ok := metadata.GetValue(metaKeyMariaDBVectorDim)
	require.True(t, ok)
	require.Equal(t, "3", dimension)

	field := arrow.Field{Name: "embedding", Type: dataType, Metadata: metadata}
	builder := array.NewBuilder(memory.DefaultAllocator, dataType)
	defer builder.Release()
	inserter, err := converter.CreateInserter(&field, builder)
	require.NoError(t, err)

	raw := make([]byte, 12)
	for i, value := range []float32{1.25, -2.5, 3.75} {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(value))
	}
	require.NoError(t, inserter.AppendValue(raw))
	require.ErrorContains(t, inserter.AppendValue(raw[:8]), "expected 12")

	result := builder.NewArray().(*array.FixedSizeList)
	defer result.Release()
	values := result.ListValues().(*array.Float32)
	require.Equal(t, []float32{1.25, -2.5, 3.75}, values.Float32Values())
	bound, err := converter.ConvertArrowToGo(result, 0, &field)
	require.NoError(t, err)
	require.Equal(t, raw, bound)
}

func TestParseMariaDBTypeValues(t *testing.T) {
	values, err := parseMariaDBTypeValues(`enum('plain','it''s','a\\b','comma,value')`)
	require.NoError(t, err)
	require.Equal(t, []string{"plain", "it's", `a\b`, "comma,value"}, values)

	_, err = parseMariaDBTypeValues(`enum('unterminated)`)
	require.Error(t, err)
}

func TestMariaDBSpatialConversionUsesGeoArrowWKB(t *testing.T) {
	converter := makeTypeConverter(zeroDatetimeBehaviorError)
	typeInfo := sqlwrapper.ColumnType{Name: "location", DatabaseTypeName: "POINT"}
	dataType, _, metadata, err := converter.ConvertRawColumnType(typeInfo)
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.Binary, dataType)
	extensionName, ok := metadata.GetValue(metaKeyLogicalArrowType)
	require.True(t, ok)
	require.Equal(t, "geoarrow.wkb", extensionName)

	field := arrow.Field{Name: "location", Type: dataType, Metadata: metadata}
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	inserter, err := converter.CreateInserter(&field, builder)
	require.NoError(t, err)
	// Four-byte SRID prefix followed by a minimal little-endian WKB point.
	internal := append([]byte{0, 0, 0, 0}, []byte{1, 1, 0, 0, 0}...)
	require.NoError(t, inserter.AppendValue(internal))
	result := builder.NewBinaryArray()
	defer result.Release()
	require.Equal(t, internal[4:], result.Value(0))

	connection := &mariadbConnectionImpl{}
	require.Equal(t, "ST_GeomFromWKB(?)", connection.GetPlaceholder(&field, 0))
}

func TestMariaDBTimestampPrecision(t *testing.T) {
	converter := makeTypeConverter(zeroDatetimeBehaviorError)
	for precision, unit := range map[int64]arrow.TimeUnit{
		0: arrow.Second, 1: arrow.Millisecond, 3: arrow.Millisecond,
		4: arrow.Microsecond, 6: arrow.Microsecond,
	} {
		type_, _, _, err := converter.ConvertRawColumnType(sqlwrapper.ColumnType{
			Name: "created", DatabaseTypeName: "TIMESTAMP", Precision: int64ptr(precision),
		})
		require.NoError(t, err)
		require.Equal(t, unit, type_.(*arrow.TimestampType).Unit)
		require.Equal(t, "UTC", type_.(*arrow.TimestampType).TimeZone)
	}
}

func TestMariaDBTimeIsSignedDuration(t *testing.T) {
	converter := makeTypeConverter(zeroDatetimeBehaviorError)
	type_, _, metadata, err := converter.ConvertRawColumnType(sqlwrapper.ColumnType{
		Name: "elapsed", DatabaseTypeName: "TIME", Precision: int64ptr(6),
	})
	require.NoError(t, err)
	require.Equal(t, &arrow.DurationType{Unit: arrow.Microsecond}, type_)

	field := arrow.Field{Name: "elapsed", Type: type_, Metadata: metadata}
	builder := array.NewDurationBuilder(memory.DefaultAllocator, type_.(*arrow.DurationType))
	defer builder.Release()
	inserter, err := converter.CreateInserter(&field, builder)
	require.NoError(t, err)
	require.NoError(t, inserter.AppendValue([]byte("-838:59:59.123456")))

	result := builder.NewDurationArray()
	defer result.Release()
	bound, err := converter.ConvertArrowToGo(result, 0, &field)
	require.NoError(t, err)
	require.Equal(t, "-838:59:59.123456", bound)

	connection := &mariadbConnectionImpl{}
	require.Equal(t, "TIME(6) NULL", connection.arrowToMariaDBType(type_, true))
}
