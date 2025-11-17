# Polarity Reducer Ex DSL Specification

This document provides a comprehensive specification for the Polarity Reducer Ex Domain Specific Language (DSL) used for data transformation pipelines.

## Table of Contents

1. [Overview](#overview)
2. [DSL Structure](#dsl-structure)
3. [Root Configuration](#root-configuration)
4. [Path Expressions](#path-expressions)
5. [Operations Reference](#operations-reference)
6. [Output Templates](#output-templates)
7. [Examples](#examples)
8. [Error Handling](#error-handling)
9. [Performance Considerations](#performance-considerations)

## Overview

The Polarity Reducer Ex DSL is a JSON-based configuration language for defining data transformation pipelines. It allows you to:

- Select working datasets from complex nested structures
- Apply sequential transformations using a pipeline approach
- Generate custom output structures with variable references
- Handle arrays, nested objects, and complex data types
- Perform date/time operations and formatting
- Clean up and validate data structures

## DSL Structure

Every DSL configuration consists of three main components:

```json
{
  "root": { /* Root configuration */ },
  "pipeline": [ /* Array of operations */ ],
  "output": { /* Output template */ }
}
```

### Component Details

- **root**: Defines the initial working dataset and error handling behavior
- **pipeline**: Sequential array of transformation operations to apply
- **output**: Template for generating the final result with variable substitution

## Root Configuration

The root configuration determines how the initial working dataset is selected from the input data.

### Schema

```json
{
  "root": {
    "path": "string",           // Path to working data (required)
    "on_null": "string"         // Null handling strategy (optional)
  }
}
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | string | `""` | JSONPath expression to select working data |
| `on_null` | string | `"return_empty"` | Behavior when path resolves to null |

### On Null Strategies

- **`"return_empty"`**: Return empty object `{}` when path is null
- **`"return_original"`**: Return the entire original input when path is null

### Examples

```json
// Work with entire input
{"root": {"path": ""}}

// Work with nested data
{"root": {"path": "data.users"}}

// Handle missing data gracefully
{"root": {"path": "optional.field", "on_null": "return_original"}}
```

## Path Expressions

Path expressions are used throughout the DSL to reference data locations. They support:

### Syntax

- **Dot notation**: `"user.profile.name"`
- **Array indices**: `"users[0].email"`
- **Array wildcards**: `"users[].name"` (applies to all elements)
- **Mixed nesting**: `"departments[].employees[].projects[].status"`

### Path Components

| Component | Description | Example |
|-----------|-------------|---------|
| `.field` | Object property access | `user.name` |
| `[index]` | Array index access | `users[0]` |
| `[]` | Array wildcard (all elements) | `users[]` |
| `.` | Path separator | `user.profile.email` |

### Advanced Path Examples

```json
// Simple object access
"user.name"

// Array element access
"users[2].profile.email"  

// All array elements
"users[].isActive"

// Nested arrays
"companies[].departments[].employees[].salary"

// Deep nesting
"config.database.connections[].pools[].settings.timeout"
```

## Operations Reference

### Core Operations

#### drop
Remove specified paths from the data structure.

**Schema:**
```json
{
  "op": "drop",
  "paths": ["string", "..."]
}
```

**Parameters:**
- `paths`: Array of path expressions to remove

**Example:**
```json
{
  "op": "drop",
  "paths": [
    "users[].internal_id",
    "metadata.debug_info",
    "config.secrets"
  ]
}
```

#### rename
Rename fields using a mapping of old paths to new paths.

**Schema:**
```json
{
  "op": "rename", 
  "mapping": {
    "old_path": "new_path",
    "...": "..."
  }
}
```

**Parameters:**
- `mapping`: Object mapping source paths to destination paths

**Example:**
```json
{
  "op": "rename",
  "mapping": {
    "users[].first_name": "users[].firstName",
    "users[].last_name": "users[].lastName",
    "metadata.created_by": "metadata.author"
  }
}
```

#### project
Extract specific fields and create new structure.

**Schema:**
```json
{
  "op": "project",
  "path": "string",
  "mapping": {
    "new_field": "source_field",
    "...": "..."
  }
}
```

**Parameters:**
- `path`: Path to the array or object to project
- `mapping`: Object mapping new field names to source field names

**Example:**
```json
{
  "op": "project",
  "path": "users",
  "mapping": {
    "id": "user_id",
    "displayName": "full_name",
    "contact": "email_address"
  }
}
```

#### prune
Remove empty, null, or unwanted values from the structure.

**Schema:**
```json
{
  "op": "prune",
  "strategy": "string"
}
```

**Parameters:**
- `strategy`: Pruning strategy (`"empty_values"` or `"null_values"`)

**Strategies:**
- `"empty_values"`: Remove null, empty strings, empty arrays, and empty objects
- `"null_values"`: Remove only null values

**Example:**
```json
{
  "op": "prune",
  "strategy": "empty_values"
}
```

### Date/Time Operations

#### current_timestamp
Add current timestamp to the data.

**Schema:**
```json
{
  "op": "current_timestamp",
  "path": "string",
  "format": "string"
}
```

**Parameters:**
- `path`: Where to place the timestamp
- `format`: Output format (`"iso8601"`, `"unix"`, `"human"`)

**Formats:**
- `"iso8601"`: `"2024-01-15T10:30:00Z"`
- `"unix"`: `1705316200` (seconds since epoch)
- `"human"`: `"2024-01-15 10:30:00"`

**Example:**
```json
{
  "op": "current_timestamp",
  "path": "metadata.processed_at",
  "format": "iso8601"
}
```

#### format_date
Format existing date fields.

**Schema:**
```json
{
  "op": "format_date",
  "path": "string",
  "format": "string"
}
```

**Parameters:**
- `path`: Path to the date field(s) to format
- `format`: Target format (`"iso8601"`, `"unix"`, `"human"`, `"date_only"`)

**Formats:**
- `"iso8601"`: `"2024-01-15T10:30:00Z"`
- `"unix"`: `1705316200`
- `"human"`: `"2024-01-15 10:30:00"`
- `"date_only"`: `"2024-01-15"`

**Example:**
```json
{
  "op": "format_date",
  "path": "users[].created_at",
  "format": "human"
}
```

#### date_add
Add time duration to date fields.

**Schema:**
```json
{
  "op": "date_add",
  "path": "string",
  "amount": number,
  "unit": "string"
}
```

**Parameters:**
- `path`: Path to the date field(s)
- `amount`: Number to add (can be negative for subtraction)
- `unit`: Time unit (`"seconds"`, `"minutes"`, `"hours"`, `"days"`, `"weeks"`, `"months"`, `"years"`)

**Example:**
```json
{
  "op": "date_add",
  "path": "users[].subscription_expires",
  "amount": 365,
  "unit": "days"
}
```

#### date_diff
Calculate difference between two date fields.

**Schema:**
```json
{
  "op": "date_diff",
  "from_path": "string",
  "to_path": "string", 
  "result_path": "string",
  "unit": "string"
}
```

**Parameters:**
- `from_path`: Source date path
- `to_path`: Target date path
- `result_path`: Where to store the result
- `unit`: Unit for the result (`"seconds"`, `"minutes"`, `"hours"`, `"days"`, `"weeks"`)

**Example:**
```json
{
  "op": "date_diff",
  "from_path": "users[].created_at",
  "to_path": "metadata.current_time",
  "result_path": "users[].account_age_days", 
  "unit": "days"
}
```

#### parse_date
Parse date strings into standardized format.

**Schema:**
```json
{
  "op": "parse_date",
  "path": "string",
  "output_format": "string"
}
```

**Parameters:**
- `path`: Path to date string(s) to parse
- `output_format`: Target format (`"iso8601"`, `"unix"`, `"human"`)

**Supported Input Formats:**
- ISO 8601: `"2024-01-15T10:30:00Z"`
- Unix timestamps: `1705316200`, `"1705316200"`
- Human readable: `"2024-01-15 10:30:00"`
- Date only: `"2024-01-15"`

**Example:**
```json
{
  "op": "parse_date", 
  "path": "events[].timestamp",
  "output_format": "iso8601"
}
```

### Advanced Operations

#### list_to_map
Convert array to map using specified key field.

**Schema:**
```json
{
  "op": "list_to_map",
  "path": "string",
  "key_field": "string"
}
```

**Parameters:**
- `path`: Path to the array
- `key_field`: Field to use as map keys

**Example:**
```json
{
  "op": "list_to_map",
  "path": "users",
  "key_field": "id"
}
```

#### aggregate_list
Perform aggregations on array data.

**Schema:**
```json
{
  "op": "aggregate_list",
  "path": "string", 
  "aggregations": {
    "result_field": {
      "field": "string",
      "function": "string"
    }
  }
}
```

**Parameters:**
- `path`: Path to the array
- `aggregations`: Object defining aggregation operations

**Functions:**
- `"sum"`: Sum numeric values
- `"avg"`: Average numeric values  
- `"min"`: Minimum value
- `"max"`: Maximum value
- `"count"`: Count items

**Example:**
```json
{
  "op": "aggregate_list",
  "path": "orders",
  "aggregations": {
    "total_amount": {"field": "amount", "function": "sum"},
    "avg_amount": {"field": "amount", "function": "avg"},
    "order_count": {"function": "count"}
  }
}
```

## Output Templates

The output section defines how to construct the final result using variable references and static values.

### Variable References

| Variable | Description | Example |
|----------|-------------|---------|
| `$working` | Transformed working data | `"$working"` |
| `$working.path` | Specific field from working data | `"$working.users[0].name"` |
| `$root` | Original input data | `"$root"` |
| `$root.path` | Specific field from original data | `"$root.metadata.version"` |

### Output Examples

```json
{
  "output": {
    "users": "$working.users",
    "metadata": {
      "processed_at": "$working.timestamp",
      "original_source": "$root.source",
      "total_count": 42
    },
    "summary": "$working.aggregations"
  }
}
```

## Examples

### Complete User Data Transformation

```json
{
  "root": {
    "path": "response.data",
    "on_null": "return_empty"
  },
  "pipeline": [
    {
      "op": "drop",
      "paths": ["users[].internal_id", "users[].debug_info"]
    },
    {
      "op": "rename", 
      "mapping": {
        "users[].first_name": "users[].firstName",
        "users[].last_name": "users[].lastName",
        "users[].email_address": "users[].email"
      }
    },
    {
      "op": "format_date",
      "path": "users[].created_at",
      "format": "human"
    },
    {
      "op": "current_timestamp",
      "path": "metadata.processed_at",
      "format": "iso8601"
    },
    {
      "op": "prune",
      "strategy": "empty_values"
    }
  ],
  "output": {
    "users": "$working.users",
    "processing_info": {
      "timestamp": "$working.metadata.processed_at",
      "source": "$root.metadata.api_version",
      "user_count": "$working.users.length"
    }
  }
}
```

### E-commerce Order Processing

```json
{
  "root": {"path": "orders"},
  "pipeline": [
    {
      "op": "format_date",
      "path": "[].order_date", 
      "format": "date_only"
    },
    {
      "op": "date_diff",
      "from_path": "[].order_date",
      "to_path": "[].delivery_date",
      "result_path": "[].delivery_days", 
      "unit": "days"
    },
    {
      "op": "aggregate_list",
      "path": "",
      "aggregations": {
        "total_revenue": {"field": "amount", "function": "sum"},
        "avg_order_value": {"field": "amount", "function": "avg"},
        "total_orders": {"function": "count"}
      }
    }
  ],
  "output": {
    "orders": "$working.orders",
    "analytics": "$working.aggregations"
  }
}
```

## Error Handling

### Common Error Types

1. **Path Resolution Errors**: Invalid or non-existent paths
2. **Type Mismatches**: Operations on incompatible data types
3. **Date Parsing Errors**: Invalid date formats
4. **Operation Errors**: Unsupported operations or parameters

### Error Response Format

```json
{
  "error": "string",
  "details": "string",
  "operation": "string",
  "path": "string"
}
```

### Best Practices

- **Validate paths**: Ensure all paths exist in your data structure
- **Handle nulls**: Use appropriate null handling strategies
- **Test incrementally**: Build pipelines step by step
- **Use prune operations**: Clean up data after transformations

## Performance Considerations

### Optimization Tips

1. **Order operations efficiently**:
   - Place `drop` operations early to reduce data size
   - Place `prune` operations at the end of pipelines
   - Group related operations together

2. **Path expression efficiency**:
   - Use specific paths instead of broad wildcards when possible
   - Avoid unnecessary deep nesting in path expressions

3. **Memory management**:
   - Process large datasets in chunks when possible
   - Use `drop` operations to remove unnecessary data early
   - Monitor memory usage for very large transformations

4. **Date operations**:
   - Parse dates once and reuse formatted values
   - Use appropriate date formats for your use case
   - Cache current timestamps when used multiple times

### Performance Benchmarks

| Dataset Size | Operations | Typical Performance |
|--------------|------------|-------------------|
| 1,000 records | Basic transformations | < 100ms |
| 5,000 records | Complex pipeline | < 1.5s |
| 10,000 records | With date operations | < 3s |
| 25,000 records | Nested arrays | < 2s |

## Version History

- **v0.1.0**: Initial release with core operations and date/time support
- **Future**: Additional aggregation functions, conditional operations, custom functions