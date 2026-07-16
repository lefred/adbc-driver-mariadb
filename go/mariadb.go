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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	// The MariaDB wire protocol is implemented by go-sql-driver/mysql, which
	// registers the internal database/sql driver name "mysql".
	_ "github.com/go-sql-driver/mysql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-ext/variant"
)

const (
	OptionKeyZeroDatetimeBehavior = "mariadb.query.zero_datetime_behavior"

	OptionValueZeroDatetimeBehaviorError         = "error"
	OptionValueZeroDatetimeBehaviorConvertToNull = "convert_to_null"
)

const (
	metaKeyMariaDBColumnType = "mariadb.column_type"
	metaKeyMariaDBEnumValues = "mariadb.enum_values"
	metaKeyMariaDBSetValues  = "mariadb.set_values"
	metaKeyMariaDBNativeType = "mariadb.native_type"
	metaKeyMariaDBVectorDim  = "mariadb.vector_dimension"
	metaKeyLogicalArrowType  = "mariadb.logical_arrow_type"
	metaKeyArrowFallback     = "mariadb.arrow_fallback"
)

type zeroDatetimeBehavior int

const (
	zeroDatetimeBehaviorError zeroDatetimeBehavior = iota
	zeroDatetimeBehaviorConvertToNull
)

func (b zeroDatetimeBehavior) String() string {
	switch b {
	case zeroDatetimeBehaviorError:
		return OptionValueZeroDatetimeBehaviorError
	case zeroDatetimeBehaviorConvertToNull:
		return OptionValueZeroDatetimeBehaviorConvertToNull
	default:
		return OptionValueZeroDatetimeBehaviorError
	}
}

func parseZeroDatetimeBehavior(value string, errorHelper *driverbase.ErrorHelper) (zeroDatetimeBehavior, error) {
	switch value {
	case OptionValueZeroDatetimeBehaviorError:
		return zeroDatetimeBehaviorError, nil
	case OptionValueZeroDatetimeBehaviorConvertToNull:
		return zeroDatetimeBehaviorConvertToNull, nil
	default:
		if errorHelper == nil {
			return zeroDatetimeBehaviorError, fmt.Errorf(
				"invalid %s value %q, expected %q or %q",
				OptionKeyZeroDatetimeBehavior,
				value,
				OptionValueZeroDatetimeBehaviorError,
				OptionValueZeroDatetimeBehaviorConvertToNull)
		}
		return zeroDatetimeBehaviorError, errorHelper.InvalidArgument(
			"invalid %s value %q, expected %q or %q",
			OptionKeyZeroDatetimeBehavior,
			value,
			OptionValueZeroDatetimeBehaviorError,
			OptionValueZeroDatetimeBehaviorConvertToNull)
	}
}

// MariaDBTypeConverter provides MariaDB-specific type conversion enhancements
type mariaDBTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
	zeroDatetimeBehavior zeroDatetimeBehavior
}

func makeTypeConverter(zeroDatetimeBehavior zeroDatetimeBehavior) sqlwrapper.TypeConverter {
	return &mariaDBTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: "MariaDB"},
		zeroDatetimeBehavior: zeroDatetimeBehavior,
	}
}

// normalizeUnsignedTypeName converts "UNSIGNED INT" -> "INT UNSIGNED" format
// The go-sql-driver/mariadb returns "UNSIGNED X" but the default type converter expects "X UNSIGNED"
func normalizeUnsignedTypeName(typeName string) string {
	if after, ok := strings.CutPrefix(typeName, "UNSIGNED "); ok {
		return after + " UNSIGNED"
	}
	return typeName
}

// ConvertRawColumnType implements TypeConverter with MariaDB-specific enhancements
func (m *mariaDBTypeConverter) ConvertRawColumnType(colType sqlwrapper.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName)
	nullable := colType.Nullable

	// Normalize "UNSIGNED X" to "X UNSIGNED" for the default type converter
	// Only update DatabaseTypeName when reordering is needed, to preserve original casing in metadata
	typeName = normalizeUnsignedTypeName(typeName)
	if typeName != strings.ToUpper(colType.DatabaseTypeName) {
		colType.DatabaseTypeName = typeName
	}

	switch typeName {
	case "UUID":
		// MariaDB returns UUID values as canonical text over the client protocol.
		// Parse them into Arrow's standard UUID extension type so consumers such
		// as DuckDB retain UUID semantics instead of degrading them to VARCHAR.
		metadata := arrow.MetadataFrom(map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		})
		return extensions.NewUUIDType(), nullable, metadata, nil

	case "INET4", "INET6":
		// MariaDB exposes network values to clients in canonical textual form.
		metadata := arrow.MetadataFrom(map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
			metaKeyMariaDBNativeType:           strings.ToLower(typeName),
			metaKeyLogicalArrowType:            "mariadb." + strings.ToLower(typeName),
		})
		return arrow.BinaryTypes.String, nullable, metadata, nil

	case "DECIMAL", "NUMERIC":
		// Arrow supports decimal256, but several ADBC consumers (including
		// DuckDB) cannot import decimals wider than 38 digits and may hide the
		// entire table when one is present. Preserve wider values losslessly as
		// UTF-8 and describe the intended logical type in field metadata.
		if colType.Precision != nil && colType.Scale != nil && *colType.Precision > 38 {
			metadata := arrow.MetadataFrom(map[string]string{
				sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
				sqlwrapper.MetaKeyColumnName:       colType.Name,
				sqlwrapper.MetaKeyPrecision:        fmt.Sprintf("%d", *colType.Precision),
				sqlwrapper.MetaKeyScale:            fmt.Sprintf("%d", *colType.Scale),
				metaKeyLogicalArrowType:            fmt.Sprintf("decimal256(%d,%d)", *colType.Precision, *colType.Scale),
				metaKeyArrowFallback:               "string",
			})
			return arrow.BinaryTypes.String, nullable, metadata, nil
		}
		return m.DefaultTypeConverter.ConvertRawColumnType(colType)

	case "VECTOR":
		// MariaDB reports the packed byte width through database/sql. Each
		// element is a little-endian IEEE-754 float32.
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
			metaKeyMariaDBNativeType:           "vector",
		}
		if colType.Length != nil && *colType.Length > 0 && *colType.Length%4 == 0 {
			dimension := *colType.Length / 4
			metadataMap[sqlwrapper.MetaKeyLength] = fmt.Sprintf("%d", *colType.Length)
			metadataMap[metaKeyMariaDBVectorDim] = fmt.Sprintf("%d", dimension)
			return arrow.FixedSizeListOf(int32(dimension), arrow.PrimitiveTypes.Float32), nullable,
				arrow.MetadataFrom(metadataMap), nil
		}
		// Some servers/clients omit the width. Preserve the bytes rather than
		// inventing a dimension.
		metadataMap["mariadb.vector_encoding"] = "float32_le"
		return arrow.BinaryTypes.Binary, nullable, arrow.MetadataFrom(metadataMap), nil

	case "JSON":
		jsonType, err := extensions.NewJSONType(arrow.BinaryTypes.String)
		if err != nil {
			return nil, false, arrow.Metadata{}, err
		}
		metadata := arrow.MetadataFrom(map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
			metaKeyMariaDBNativeType:           "json",
		})
		return jsonType, nullable, metadata, nil

	case "YEAR":
		metadata := arrow.MetadataFrom(map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
			metaKeyMariaDBNativeType:           "year",
		})
		return arrow.PrimitiveTypes.Int16, nullable, metadata, nil

	case "TIME":
		unit := arrow.Microsecond
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
			metaKeyMariaDBNativeType:           "time_duration",
		}
		if colType.Precision != nil {
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", *colType.Precision)
			switch {
			case *colType.Precision <= 0:
				unit = arrow.Second
			case *colType.Precision <= 3:
				unit = arrow.Millisecond
			}
		}
		return &arrow.DurationType{Unit: unit}, nullable, arrow.MetadataFrom(metadataMap), nil

	case "BIT":
		// Handle BIT type as binary data
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.Binary, nullable, metadata, nil

	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		// Convert MariaDB spatial types to binary with spatial metadata
		// TODO: we should use geoarrow extension types if applicable
		metadata := arrow.MetadataFrom(map[string]string{
			"sql.database_type_name":    colType.DatabaseTypeName,
			"sql.column_name":           colType.Name,
			"mariadb.is_spatial":        "true",
			"mariadb.geometry_encoding": "internal_srid_wkb",
			metaKeyLogicalArrowType:     "geoarrow.wkb",
		})
		return arrow.BinaryTypes.Binary, nullable, metadata, nil

	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
			"mariadb.is_enum_set":    "true",
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, nullable, metadata, nil

	case "TIMESTAMP":
		var timestampType arrow.DataType
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			precision := *colType.Precision
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			var timeUnit arrow.TimeUnit
			switch {
			case precision <= 0:
				timeUnit = arrow.Second
			case precision <= 3:
				timeUnit = arrow.Millisecond
			default:
				timeUnit = arrow.Microsecond
			}
			timestampType = &arrow.TimestampType{Unit: timeUnit, TimeZone: "UTC"}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, colType.Nullable, metadata, nil

	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertRawColumnType(colType)
	}
}

// CreateInserter creates MariaDB-specific inserters bound to builders for enhanced performance
func (m *mariaDBTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for MariaDB-specific types first
	switch field.Type.(type) {
	case *extensions.JSONType:
		return &mariadbJSONInserter{builder: builder}, nil
	case *extensions.UUIDType:
		return &mariadbUUIDInserter{builder: builder}, nil
	case *arrow.BinaryType:
		if dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name"); ok && dbTypeName == "BIT" {
			return &mariadbBitInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
		}
		// Handle MariaDB spatial types
		if isSpatial, ok := field.Metadata.GetValue("mariadb.is_spatial"); ok && isSpatial == "true" {
			return &mariadbSpatialInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
		}
		// Fall through to default for non-spatial binary
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	case *arrow.FixedSizeListType:
		if nativeType, ok := field.Metadata.GetValue(metaKeyMariaDBNativeType); ok && nativeType == "vector" {
			return newMariaDBVectorInserter(builder)
		}
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	case *arrow.DurationType:
		if nativeType, ok := field.Metadata.GetValue(metaKeyMariaDBNativeType); ok && nativeType == "time_duration" {
			return &mariaDBDurationInserter{builder: builder.(*array.DurationBuilder)}, nil
		}
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	case *arrow.Date32Type:
		defaultInserter, err := m.DefaultTypeConverter.CreateInserter(field, builder)
		if err != nil {
			return nil, err
		}
		return &mariadbZeroDatetimeInserter{
			builder:              builder,
			defaultInserter:      defaultInserter,
			zeroDatetimeBehavior: m.zeroDatetimeBehavior,
		}, nil
	case *arrow.Date64Type:
		defaultInserter, err := m.DefaultTypeConverter.CreateInserter(field, builder)
		if err != nil {
			return nil, err
		}
		return &mariadbZeroDatetimeInserter{
			builder:              builder,
			defaultInserter:      defaultInserter,
			zeroDatetimeBehavior: m.zeroDatetimeBehavior,
		}, nil
	case *arrow.TimestampType:
		defaultInserter, err := m.DefaultTypeConverter.CreateInserter(field, builder)
		if err != nil {
			return nil, err
		}
		return &mariadbZeroDatetimeInserter{
			builder:              builder,
			defaultInserter:      defaultInserter,
			zeroDatetimeBehavior: m.zeroDatetimeBehavior,
		}, nil
	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

// MariaDB-specific inserters
type mariadbJSONInserter struct {
	builder array.Builder
}

type mariadbUUIDInserter struct {
	builder array.Builder
}

func (ins *mariadbUUIDInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	var value string
	switch v := sqlValue.(type) {
	case []byte:
		value = string(v)
	case string:
		value = v
	default:
		return fmt.Errorf("expected []byte or string for mariadb uuid inserter, got %T", sqlValue)
	}
	return ins.builder.AppendValueFromString(value)
}

func (ins *mariadbJSONInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	var value string
	switch v := sqlValue.(type) {
	case []byte:
		value = string(v)
	case string:
		value = v
	default:
		return fmt.Errorf("expected []byte or string for mariadb json inserter, got %T", sqlValue)
	}

	// For extension types, we need to use AppendValueFromString
	// since the ExtensionBuilder doesn't implement StringLikeBuilder.Append
	return ins.builder.AppendValueFromString(value)
}

type mariadbBitInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *mariadbBitInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for mariadb bit inserter, got %T", sqlValue)
	}

	ins.builder.Append(t)
	return nil
}

type mariadbSpatialInserter struct {
	builder array.BinaryLikeBuilder
}

type mariadbVectorInserter struct {
	builder *array.FixedSizeListBuilder
	values  *array.Float32Builder
	dim     int
}

type mariaDBDurationInserter struct {
	builder *array.DurationBuilder
}

func (ins *mariaDBDurationInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}
	var text string
	switch value := sqlValue.(type) {
	case []byte:
		text = string(value)
	case string:
		text = value
	default:
		return fmt.Errorf("expected []byte or string for MariaDB TIME, got %T", sqlValue)
	}
	duration, err := parseMariaDBDuration(text)
	if err != nil {
		return err
	}
	unit := ins.builder.Type().(*arrow.DurationType).Unit
	ins.builder.Append(arrow.Duration(int64(duration) / int64(unit.Multiplier())))
	return nil
}

func parseMariaDBDuration(value string) (time.Duration, error) {
	sign := int64(1)
	if strings.HasPrefix(value, "-") {
		sign = -1
		value = value[1:]
	}
	parts := strings.SplitN(value, ".", 2)
	clock := strings.Split(parts[0], ":")
	if len(clock) != 3 {
		return 0, fmt.Errorf("invalid MariaDB TIME value %q", value)
	}
	hours, err1 := strconv.ParseInt(clock[0], 10, 64)
	minutes, err2 := strconv.ParseInt(clock[1], 10, 64)
	seconds, err3 := strconv.ParseInt(clock[2], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || hours > 838 || minutes > 59 || seconds > 59 {
		return 0, fmt.Errorf("invalid MariaDB TIME value %q", value)
	}
	microseconds := int64(0)
	if len(parts) == 2 {
		fraction := parts[1]
		if len(fraction) > 6 {
			fraction = fraction[:6]
		}
		fraction += strings.Repeat("0", 6-len(fraction))
		microseconds, err1 = strconv.ParseInt(fraction, 10, 64)
		if err1 != nil {
			return 0, fmt.Errorf("invalid MariaDB TIME value %q", value)
		}
	}
	total := time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second + time.Duration(microseconds)*time.Microsecond
	return time.Duration(sign) * total, nil
}

func newMariaDBVectorInserter(builder array.Builder) (*mariadbVectorInserter, error) {
	b, ok := builder.(*array.FixedSizeListBuilder)
	if !ok {
		return nil, fmt.Errorf("expected fixed-size-list builder for MariaDB VECTOR, got %T", builder)
	}
	values, ok := b.ValueBuilder().(*array.Float32Builder)
	if !ok {
		return nil, fmt.Errorf("expected float32 child builder for MariaDB VECTOR, got %T", b.ValueBuilder())
	}
	dim := int(b.Type().(*arrow.FixedSizeListType).Len())
	return &mariadbVectorInserter{builder: b, values: values, dim: dim}, nil
}

func (ins *mariadbVectorInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}
	var raw []byte
	switch value := sqlValue.(type) {
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		return fmt.Errorf("expected []byte or string for MariaDB VECTOR, got %T", sqlValue)
	}
	if len(raw) != ins.dim*4 {
		return fmt.Errorf("MariaDB VECTOR has %d bytes, expected %d for VECTOR(%d)", len(raw), ins.dim*4, ins.dim)
	}
	ins.builder.Append(true)
	for offset := 0; offset < len(raw); offset += 4 {
		ins.values.Append(math.Float32frombits(binary.LittleEndian.Uint32(raw[offset : offset+4])))
	}
	return nil
}

func (ins *mariadbSpatialInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for mariadb spatial inserter, got %T", sqlValue)
	}

	// MariaDB's binary geometry representation normally prefixes standard WKB
	// with a four-byte SRID. GeoArrow WKB contains only the WKB payload.
	if len(t) >= 5 && (t[4] == 0 || t[4] == 1) {
		t = t[4:]
	}
	ins.builder.Append(t)
	return nil
}

type mariadbZeroDatetimeInserter struct {
	builder              array.Builder
	defaultInserter      sqlwrapper.Inserter
	zeroDatetimeBehavior zeroDatetimeBehavior
}

func (ins *mariadbZeroDatetimeInserter) AppendValue(sqlValue any) error {
	isZeroDatetime, err := isZeroDatetimeValue(sqlValue)
	if err != nil {
		return err
	}
	if !isZeroDatetime {
		return ins.defaultInserter.AppendValue(sqlValue)
	}

	switch ins.zeroDatetimeBehavior {
	case zeroDatetimeBehaviorError:
		return adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  "zero datetime value cannot be converted to Arrow date or timestamp",
		}
	case zeroDatetimeBehaviorConvertToNull:
		ins.builder.AppendNull()
		return nil
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  "zero datetime value cannot be converted to Arrow date or timestamp",
		}
	}
}

func isZeroDatetimeValue(sqlValue any) (bool, error) {
	switch v := sqlValue.(type) {
	case nil:
		return false, nil
	case []byte:
		return hasZeroDatePrefix(string(v)), nil
	case string:
		return hasZeroDatePrefix(v), nil
	default:
		return false, nil
	}
}

func hasZeroDatePrefix(value string) bool {
	if len(value) < len("0000-00-00") {
		return false
	}
	if value[4] != '-' || value[7] != '-' {
		return false
	}

	year := value[:4]
	month := value[5:7]
	day := value[8:10]
	return year == "0000" || month == "00" || day == "00"
}

// ConvertArrowToGo implements MariaDB-specific Arrow value to Go value conversion
func (m *mariaDBTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle MariaDB-specific Arrow to Go conversions
	switch a := arrowArray.(type) {
	case *extensions.JSONArray:
		// Handle JSON extension type arrays
		jsonStr := a.ValueStr(index)
		v := variant.New(jsonStr)
		return v, nil

	case *extensions.UUIDArray:
		return a.Value(index).String(), nil

	case *array.Time32:
		// For MariaDB driver, always convert Time32 arrays to time-only format strings
		// This handles both explicit TIME column metadata and parameter binding scenarios
		timeType := a.DataType().(*arrow.Time32Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000"), nil

	case *array.Time64:
		// For MariaDB driver, always convert Time64 arrays to time-only format strings
		// This handles both explicit TIME column metadata and parameter binding scenarios
		timeType := a.DataType().(*arrow.Time64Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000"), nil

	case *array.Timestamp:
		timestampType := a.DataType().(*arrow.TimestampType)
		rawValue := a.Value(index)
		t := rawValue.ToTime(timestampType.Unit)

		// For nanosecond precision, truncate to microseconds
		if timestampType.Unit == arrow.Nanosecond {
			microseconds := t.UnixMicro()
			converted := time.UnixMicro(microseconds).UTC()
			return converted, nil
		}

		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	case *array.FixedSizeList:
		if nativeType, ok := field.Metadata.GetValue(metaKeyMariaDBNativeType); ok && nativeType == "vector" {
			values, ok := a.ListValues().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("MariaDB VECTOR requires float32 values, got %T", a.ListValues())
			}
			dimension := int(a.DataType().(*arrow.FixedSizeListType).Len())
			raw := make([]byte, dimension*4)
			start := index * dimension
			for i := 0; i < dimension; i++ {
				binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(values.Value(start+i)))
			}
			return raw, nil
		}
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	case *array.Duration:
		if nativeType, ok := field.Metadata.GetValue(metaKeyMariaDBNativeType); ok && nativeType == "time_duration" {
			unit := a.DataType().(*arrow.DurationType).Unit
			duration := time.Duration(a.Value(index)) * unit.Multiplier()
			sign := ""
			if duration < 0 {
				sign = "-"
				duration = -duration
			}
			hours := int64(duration / time.Hour)
			duration %= time.Hour
			minutes := int64(duration / time.Minute)
			duration %= time.Minute
			seconds := int64(duration / time.Second)
			microseconds := int64(duration%time.Second) / int64(time.Microsecond)
			return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hours, minutes, seconds, microseconds), nil
		}
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	case *array.Float16:
		return a.Value(index).Float32(), nil

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
}

// mariadbConnectionImpl extends sqlwrapper connection with MariaDB-specific functionality
type mariadbConnectionImpl struct {
	*sqlwrapper.ConnectionImplBase // Embed sqlwrapper connection for all standard functionality

	version              string
	zeroDatetimeBehavior zeroDatetimeBehavior
}

// implements BulkIngester interface
var _ sqlwrapper.BulkIngester = (*mariadbConnectionImpl)(nil)

// implements CurrentNameSpacer interface
var _ driverbase.CurrentNamespacer = (*mariadbConnectionImpl)(nil)

// implements TableTypeLister interface
var _ driverbase.TableTypeLister = (*mariadbConnectionImpl)(nil)

// implements AutocommitSetter interface
var _ driverbase.AutocommitSetter = (*mariadbConnectionImpl)(nil)

// mariadbConnectionFactory creates MariaDB connections
type mariadbConnectionFactory struct {
}

func (f *mariadbConnectionFactory) CreateDatabase(database *sqlwrapper.DatabaseImplBase) (sqlwrapper.DatabaseImpl, error) {
	return &mariadbDatabase{
		DatabaseImplBase:     database,
		zeroDatetimeBehavior: zeroDatetimeBehaviorError,
	}, nil
}

func (f *mariadbConnectionFactory) CreateConnection(
	ctx context.Context,
	conn *sqlwrapper.ConnectionImplBase,
) (sqlwrapper.ConnectionImpl, error) {
	// Wrap the pre-built sqlwrapper connection with MariaDB-specific functionality
	return &mariadbConnectionImpl{
		ConnectionImplBase:   conn,
		zeroDatetimeBehavior: conn.Database.Derived.(*mariadbDatabase).zeroDatetimeBehavior,
	}, nil
}

func (f *mariadbConnectionFactory) CreateStatement(stmt *sqlwrapper.StatementImplBase) (sqlwrapper.StatementImpl, error) {
	return &mariadbStatement{
		StatementImplBase:    stmt,
		zeroDatetimeBehavior: stmt.Conn.Derived.(*mariadbConnectionImpl).zeroDatetimeBehavior,
	}, nil
}

type mariadbDatabase struct {
	*sqlwrapper.DatabaseImplBase
	zeroDatetimeBehavior zeroDatetimeBehavior
}

func (db *mariadbDatabase) SetOptions(ctx context.Context, opts map[string]string) error {
	for key, value := range opts {
		if err := db.SetOption(ctx, key, value); err != nil {
			return err
		}
	}
	return nil
}

func (db *mariadbDatabase) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		return db.zeroDatetimeBehavior.String(), nil
	default:
		return db.DatabaseImplBase.GetOption(ctx, key)
	}
}

func (db *mariadbDatabase) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		behavior, err := parseZeroDatetimeBehavior(value, &db.ErrorHelper)
		if err != nil {
			return err
		}
		db.zeroDatetimeBehavior = behavior
		return nil
	default:
		return db.DatabaseImplBase.SetOption(ctx, key, value)
	}
}

type mariadbStatement struct {
	*sqlwrapper.StatementImplBase
	zeroDatetimeBehavior zeroDatetimeBehavior
	query                string
	prepared             bool
}

func (s *mariadbStatement) MakeTypeConverter(vendorName string) sqlwrapper.TypeConverter {
	return makeTypeConverter(s.zeroDatetimeBehavior)
}

func (s *mariadbStatement) SetSqlQuery(ctx context.Context, query string) error {
	if err := s.StatementImplBase.SetSqlQuery(ctx, query); err != nil {
		return err
	}
	s.query = query
	s.prepared = false
	return nil
}

func (s *mariadbStatement) Prepare(ctx context.Context) error {
	if err := s.StatementImplBase.Prepare(ctx); err != nil {
		s.prepared = false
		return err
	}
	s.prepared = true
	return nil
}

// GetParameterSchema reports the number and order of positional parameters.
// The database/sql API exposes neither the parameter definitions nor their
// inferred types from COM_STMT_PREPARE, so ADBC requires null-typed fields.
func (s *mariadbStatement) GetParameterSchema(context.Context) (*arrow.Schema, error) {
	if !s.prepared {
		return nil, s.Base().ErrorHelper.InvalidState("statement must be prepared before getting parameter schema")
	}

	fields := make([]arrow.Field, countPlaceholders(s.query))
	for i := range fields {
		fields[i] = arrow.Field{Type: arrow.Null, Nullable: true}
	}
	return arrow.NewSchema(fields, nil), nil
}

// countPlaceholders counts positional '?' parameters while ignoring quoted
// strings, quoted identifiers, and SQL comments.
func countPlaceholders(query string) int {
	const (
		plain = iota
		singleQuoted
		doubleQuoted
		backtickQuoted
		lineComment
		blockComment
	)

	state, count := plain, 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch state {
		case plain:
			switch ch {
			case '?':
				count++
			case '\'':
				state = singleQuoted
			case '"':
				state = doubleQuoted
			case '`':
				state = backtickQuoted
			case '#':
				state = lineComment
			case '-':
				if i+2 < len(query) && query[i+1] == '-' && query[i+2] <= ' ' {
					state = lineComment
					i++
				}
			case '/':
				if i+1 < len(query) && query[i+1] == '*' {
					state = blockComment
					i++
				}
			}
		case singleQuoted, doubleQuoted, backtickQuoted:
			quote := byte('\'')
			if state == doubleQuoted {
				quote = '"'
			} else if state == backtickQuoted {
				quote = '`'
			}
			if ch == '\\' && state != backtickQuoted && i+1 < len(query) {
				i++
			} else if ch == quote {
				if i+1 < len(query) && query[i+1] == quote {
					i++
				} else {
					state = plain
				}
			}
		case lineComment:
			if ch == '\n' || ch == '\r' {
				state = plain
			}
		case blockComment:
			if ch == '*' && i+1 < len(query) && query[i+1] == '/' {
				state = plain
				i++
			}
		}
	}
	return count
}

func (c *mariadbConnectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	stmt, err := c.ConnectionImplBase.NewStatement(ctx)
	if err != nil {
		return nil, err
	}
	if err := stmt.SetOption(ctx, OptionKeyZeroDatetimeBehavior, c.zeroDatetimeBehavior.String()); err != nil {
		closeErr := stmt.Close(ctx)
		if closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, err
	}
	return stmt, nil
}

func (c *mariadbConnectionImpl) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		return c.zeroDatetimeBehavior.String(), nil
	case adbc.OptionKeyIsolationLevel:
		if err := c.ClearPending(); err != nil {
			return "", err
		}
		var level string
		if err := c.Conn.QueryRowContext(ctx, "SELECT @@SESSION.transaction_isolation").Scan(&level); err != nil {
			return "", c.ErrorHelper.WrapIO(err, "failed to get transaction isolation level")
		}
		switch strings.ToUpper(strings.ReplaceAll(level, "-", " ")) {
		case "READ UNCOMMITTED":
			return string(adbc.LevelReadUncommitted), nil
		case "READ COMMITTED":
			return string(adbc.LevelReadCommitted), nil
		case "REPEATABLE READ":
			return string(adbc.LevelRepeatableRead), nil
		case "SERIALIZABLE":
			return string(adbc.LevelSerializable), nil
		default:
			return "", c.ErrorHelper.Internal("unknown MariaDB transaction isolation level %q", level)
		}
	default:
		return c.ConnectionImplBase.GetOption(ctx, key)
	}
}

func (c *mariadbConnectionImpl) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		behavior, err := parseZeroDatetimeBehavior(value, &c.Base().ErrorHelper)
		if err != nil {
			return err
		}
		c.zeroDatetimeBehavior = behavior
		return nil
	case adbc.OptionKeyIsolationLevel:
		return c.setIsolationLevel(ctx, value)
	default:
		return c.ConnectionImplBase.SetOption(ctx, key, value)
	}
}

func (c *mariadbConnectionImpl) setIsolationLevel(ctx context.Context, value string) error {
	if err := c.ClearPending(); err != nil {
		return err
	}

	var query string
	switch adbc.OptionIsolationLevel(value) {
	case adbc.LevelDefault:
		query = "SET SESSION transaction_isolation = DEFAULT"
	case adbc.LevelReadUncommitted:
		query = "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
	case adbc.LevelReadCommitted:
		query = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"
	case adbc.LevelRepeatableRead:
		query = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	case adbc.LevelSerializable:
		query = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"
	case adbc.LevelSnapshot, adbc.LevelLinearizable:
		return c.ErrorHelper.Errorf(adbc.StatusNotImplemented,
			"transaction isolation level %q is not supported by MariaDB", value)
	default:
		return c.ErrorHelper.InvalidArgument("invalid transaction isolation level %q", value)
	}

	if _, err := c.Conn.ExecContext(ctx, query); err != nil {
		return c.ErrorHelper.WrapIO(err, "failed to set transaction isolation level")
	}
	return nil
}

// SetAutocommit updates the transaction mode for this connection's dedicated
// MariaDB session.  MariaDB starts a new transaction implicitly when the next
// statement executes while autocommit is disabled.
func (c *mariadbConnectionImpl) SetAutocommit(ctx context.Context, enabled bool) error {
	if err := c.ClearPending(); err != nil {
		return err
	}

	value := "0"
	if enabled {
		value = "1"
	}
	if _, err := c.Conn.ExecContext(ctx, "SET autocommit = "+value); err != nil {
		return c.ErrorHelper.WrapIO(err, "failed to set autocommit to %t", enabled)
	}
	return nil
}

func (c *mariadbConnectionImpl) Commit(ctx context.Context) error {
	if err := c.ClearPending(); err != nil {
		return err
	}
	if _, err := c.Conn.ExecContext(ctx, "COMMIT"); err != nil {
		return c.ErrorHelper.WrapIO(err, "failed to commit transaction")
	}
	return nil
}

func (c *mariadbConnectionImpl) Rollback(ctx context.Context) error {
	if err := c.ClearPending(); err != nil {
		return err
	}
	if _, err := c.Conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return c.ErrorHelper.WrapIO(err, "failed to roll back transaction")
	}
	return nil
}

func (s *mariadbStatement) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		return s.zeroDatetimeBehavior.String(), nil
	default:
		return s.StatementImplBase.GetOption(ctx, key)
	}
}

func (s *mariadbStatement) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		behavior, err := parseZeroDatetimeBehavior(value, &s.Base().ErrorHelper)
		if err != nil {
			return err
		}
		s.zeroDatetimeBehavior = behavior
		return nil
	default:
		return s.StatementImplBase.SetOption(ctx, key, value)
	}
}

// infoSqlIdentifierQuoteChar is the Flight SQL GetSqlInfo code for
// SQL_IDENTIFIER_QUOTE_CHAR, in the [500, 1000) XDBC range reserved by ADBC.
const infoSqlIdentifierQuoteChar = 504

// NewDriver constructs the ADBC Driver for "mariadb".
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	factory := &mariadbConnectionFactory{}
	// "mysql" is the database/sql registration name of the protocol library;
	// all public ADBC identity and configuration remains MariaDB-specific.
	driver := sqlwrapper.NewDriver(alloc, "mysql", "MariaDB", NewMariaDBDBFactory()).
		WithDatabaseFactory(factory).
		WithConnectionFactory(factory).
		WithStatementFactory(factory).
		WithErrorInspector(MariaDBErrorInspector{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:                       "ADBC Driver Foundry Driver for MariaDB",
		adbc.InfoVendorSql:                        true,
		adbc.InfoVendorSubstrait:                  false,
		adbc.InfoCode(infoSqlIdentifierQuoteChar): "`",
	})
	return driver
}
