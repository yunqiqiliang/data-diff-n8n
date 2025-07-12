# ClickZetta Type Support Update

## Overview

Updated the ClickZetta dialect in data-diff to support all data types available in clickzetta-connector v0.8.78.16.

## Changes Made

### 1. Added Native ClickZetta Type Names

ClickZetta uses explicit bit-width naming for numeric types, similar to modern columnar databases:

- **INT8** (8-bit integer) - Previously only supported via TINYINT alias
- **INT16** (16-bit integer) - Previously only supported via SMALLINT alias  
- **INT32** (32-bit integer) - Previously only supported via INT alias
- **INT64** (64-bit integer) - Previously only supported via BIGINT alias
- **FLOAT32** (32-bit float) - Previously only supported via FLOAT alias
- **FLOAT64** (64-bit float) - Previously only supported via DOUBLE alias

### 2. Added Missing Types

- **BINARY** - Binary data type, mapped to Text for comparison purposes
- **TIMESTAMP_LTZ** - Internal representation of timestamps with local timezone
- **ARRAY** - Array type (recognized but not fully supported for comparison)
- **MAP** - Key-value pairs (recognized but not fully supported for comparison)
- **STRUCT** - Structured data with named fields (recognized but not fully supported for comparison)
- **INTERVAL_YEAR_MONTH** - Year-month intervals (recognized but not supported)
- **INTERVAL_DAY_TIME** - Day-time intervals (recognized but not supported)
- **BITMAP** - Special type for set operations (recognized but not supported)

### 3. Improved Type Parsing

Updated the `_process_table_schema` method to better handle parameterized types:
- Improved parsing of types with parameters like DECIMAL(p,s), VARCHAR(n), CHAR(n)
- More robust handling of various type formats returned by ClickZetta

## Type Mapping Details

### Fully Supported Types (can be compared)

| ClickZetta Type | Data-Diff Type | Notes |
|-----------------|----------------|-------|
| INT8, TINYINT | Integer | 8-bit signed integer |
| INT16, SMALLINT | Integer | 16-bit signed integer |
| INT32, INT | Integer | 32-bit signed integer |
| INT64, BIGINT | Integer | 64-bit signed integer |
| FLOAT32, FLOAT | Float | 32-bit floating point |
| FLOAT64, DOUBLE | Float | 64-bit floating point |
| DECIMAL(p,s) | Decimal | Fixed-point with precision and scale |
| BOOLEAN | Boolean | True/false values |
| DATE | Date | Calendar date |
| TIMESTAMP, TIMESTAMP_LTZ | Timestamp | Point in time (no timezone support) |
| STRING, VARCHAR(n), CHAR(n) | Text | Character strings |
| BINARY | Text | Binary data (compared as text) |

### Recognized but Not Fully Supported

These types are recognized to prevent errors but mapped to UnknownColType:
- ARRAY - Complex type for ordered collections
- MAP - Complex type for key-value pairs
- STRUCT - Complex type for structured data
- INTERVAL_YEAR_MONTH - Interval type
- INTERVAL_DAY_TIME - Interval type
- BITMAP - Special type for set operations

## Usage Notes

1. **Binary Data**: BINARY type is mapped to Text for comparison. This allows basic equality checking but may not be suitable for all binary data comparisons.

2. **Complex Types**: While ARRAY, MAP, and STRUCT are recognized, they cannot be effectively compared. Consider extracting specific fields or converting to JSON strings for comparison.

3. **Timezone Reminder**: ClickZetta does not support timezones. All TIMESTAMP data must be pre-converted to UTC before comparison with other databases.

4. **Type Aliases**: Both traditional SQL aliases (INT, FLOAT) and ClickZetta native names (INT32, FLOAT32) are supported for backward compatibility.

## Testing Recommendations

When using the updated type support:

1. Test binary column comparisons carefully, especially with non-text binary data
2. For complex types (ARRAY, MAP, STRUCT), consider alternative comparison strategies
3. Always verify timezone consistency when comparing timestamps
4. Use appropriate precision settings for DECIMAL comparisons

## Future Improvements

1. Consider adding proper Binary type to data-diff core for better binary data handling
2. Implement specialized comparison logic for complex types
3. Add automatic type conversion warnings for incompatible type comparisons