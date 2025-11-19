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
- Transform data with built-in functions (uppercase, lowercase, type conversions, etc.)
- Copy and move data between paths with advanced array support
- Merge data from multiple sources with flexible strategies

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

#### set
Set field values to static values or copy from other paths.

**Schema:**
```json
{
  "op": "set",
  "path": "string",
  "value": any
}
```

**Parameters:**
- `path`: Path where to set the value
- `value`: Value to set (static value or path reference)

**Value Types:**
- **Static values**: Numbers, strings, booleans, objects, arrays
- **Path references**: `"$path:source.field"` to copy from another field

**Examples:**
```json
{
  "op": "set",
  "path": "metadata.processed",
  "value": true
}
```

```json
{
  "op": "set", 
  "path": "users[].display_name",
  "value": "$path:users[].first_name"
}
```

```json
{
  "op": "set",
  "path": "summary.stats",
  "value": {
    "total_users": 0,
    "active": false
  }
}
```

#### transform
Apply transformation functions to field values.

**Schema:**
```json
{
  "op": "transform",
  "path": "string",
  "function": "string",
  "args": ["string", "..."]
}
```

**Parameters:**
- `path`: Path to the field(s) to transform
- `function`: Transformation function name
- `args`: Optional array of function arguments

**Supported Functions:**

**String Functions:**
- `"uppercase"`: Convert to uppercase
- `"lowercase"`: Convert to lowercase  
- `"capitalize"`: Capitalize first letter 
- `"trim"`: Remove leading/trailing whitespace
- `"reverse"`: Reverse string
- `"split"`: Split string into array (args: delimiter, default: " ")

**Type Conversion Functions:**
- `"string"`: Convert to string
- `"number"`: Convert to number (integer or float)
- `"integer"`: Convert to integer
- `"float"`: Convert to float
- `"boolean"`: Convert to boolean

**Utility Functions:**
- `"length"`: Get length of string, array, or map
- `"reverse"`: Reverse string or array
- `"join"`: Join array into string (args: delimiter, default: " ")
- `"abs"`: Absolute value of number
- `"round"`: Round number (args: precision, default: 0)

**Examples:**
```json
{
  "op": "transform",
  "path": "user.name",
  "function": "uppercase"
}
```

```json
{
  "op": "transform", 
  "path": "users[].email",
  "function": "lowercase"
}
```

```json
{
  "op": "transform",
  "path": "description",
  "function": "split",
  "args": [","]
}
```

```json
{
  "op": "transform",
  "path": "scores[].value",
  "function": "round",
  "args": [2]
}
```

#### copy
Copy values from one path to another without removing the original.

**Schema:**
```json
{
  "op": "copy",
  "from": "string",
  "to": "string"
}
```

**Parameters:**
- `from`: Source path to copy from
- `to`: Destination path to copy to

**Array Behavior:**
- **Same Array Element-wise**: `users[].email` → `users[].backup_email` copies element by element
- **Array to Regular**: `users[].name` → `summary.names` copies entire array result
- **Regular to Array**: `template` → `items[].template` copies value to all array elements

**Examples:**
```json
{
  "op": "copy",
  "from": "user.email",
  "to": "backup.user_email"
}
```

```json
{
  "op": "copy",
  "from": "users[].name", 
  "to": "users[].display_name"
}
```

```json
{
  "op": "copy",
  "from": "config.default_theme",
  "to": "users[].theme"
}
```

#### move
Move values from one path to another, removing from the original location (atomic copy + drop).

**Schema:**
```json
{
  "op": "move",
  "from": "string",
  "to": "string"
}
```

**Parameters:**
- `from`: Source path to move from (original will be removed)
- `to`: Destination path to move to

**Array Behavior:**
- **Same Array Element-wise**: `users[].temp_email` → `users[].email` moves element by element within the same array
- **Array to Regular**: `users[].name` → `summary.names` moves entire array result and removes original fields
- **Regular to Array**: `template` → `items[].template` moves value to all array elements and removes original
- **Cross-Array**: `products[].name` → `inventory[].product_name` moves array result to target and removes original fields

**Examples:**
```json
{
  "op": "move",
  "from": "user.temp_email",
  "to": "user.email"
}
```

```json
{
  "op": "move",
  "from": "users[].draft_name",
  "to": "users[].name"
}
```

```json
{
  "op": "move",
  "from": "config.temp_settings",
  "to": "items[].settings"
}
```

```json
{
  "op": "move",
  "from": "old_data.users",
  "to": "migrated.user_list"
}
```

#### merge
Combine values from multiple source paths into a single target location. Only performs shallow merges of maps.

**Schema:**
```json
{
  "op": "merge",
  "sources": ["string"],
  "to": "string"
}
```

**Parameters:**
- `sources`: Array of source paths to merge from
- `to`: Destination path for the merged result

**Merge Behavior:**
- **Maps**: Merges all map objects using shallow merge (last wins for conflicting keys)
- **Non-Maps**: Returns the last non-nil value (arrays, strings, numbers, etc.)
- **Nil Handling**: Ignores nil and missing sources

**Examples:**
```json
{
  "op": "merge",
  "sources": ["config.defaults", "config.overrides"],
  "to": "config.final"
}
```

```json
{
  "op": "merge",
  "sources": ["user.profile", "user.preferences", "user.settings"],
  "to": "user.merged_config"
}
```

```json
{
  "op": "merge",
  "sources": ["source1", "source2", "source3"],
  "to": "target.combined"
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
Perform aggregations on array data using special variables.

**Schema:**
```json
{
  "op": "aggregate_list",
  "path": "string", 
  "shape": {
    "result_field": "string"
  }
}
```

**Parameters:**
- `path`: Path to the array to aggregate
- `shape`: Object defining output structure with aggregation variables

**Supported Aggregation Variables:**
- `$min(field)`: Find minimum value of specified field across array items
- `$max(field)`: Find maximum value of specified field across array items

**Example:**
```json
{
  "op": "aggregate_list",
  "path": "orders",
  "shape": {
    "min_amount": "$min(amount)",
    "max_amount": "$max(amount)",
    "description": "Order statistics"
  }
}
```

**Note:** Currently only `$min()` and `$max()` aggregation functions are implemented. The operation replaces the array at the specified path with the aggregated result object.

#### hoist_map_values
Move values from a nested map to the parent level.

**Schema:**
```json
{
  "op": "hoist_map_values",
  "path": "string",
  "child_key": "string",
  "replace_parent": "boolean"
}
```

**Parameters:**
- `path`: Path to the parent map containing the nested map
- `child_key`: Key of the nested map whose values should be hoisted
- `replace_parent`: If true, replaces parent with child values; if false, merges them

**Example:**
```json
{
  "op": "hoist_map_values",
  "path": "user",
  "child_key": "profile",
  "replace_parent": true
}
```

#### list_to_dynamic_map
Convert array to a map by grouping items by a key field.

**Schema:**
```json
{
  "op": "list_to_dynamic_map",
  "path": "string",
  "key_from": "string",
  "value_from": "string"
}
```

**Parameters:**
- `path`: Path to the array to transform
- `key_from`: Field to use as grouping key
- `value_from`: Field to extract as values (creates arrays of values for each key)

**Example:**
```json
{
  "op": "list_to_dynamic_map",
  "path": "orders",
  "key_from": "status",
  "value_from": "amount"
}
```

#### project_and_replace
Replace the entire working data with a projected subset.

**Schema:**
```json
{
  "op": "project_and_replace",
  "projection": {
    "new_field": "source_field",
    "...": "..."
  }
}
```

**Parameters:**
- `projection`: Object mapping new field names to source paths

**Example:**
```json
{
  "op": "project_and_replace",
  "projection": {
    "id": "user.id",
    "name": "user.profile.name",
    "email": "user.contact.email"
  }
}
```

#### promote_list_to_keys
Convert array items to key-value pairs at the parent level.

**Schema:**
```json
{
  "op": "promote_list_to_keys",
  "path": "string",
  "child_list": "string",
  "key_from": "string",
  "value_from": "string"
}
```

**Parameters:**
- `path`: Path to the parent object
- `child_list`: Name of the array field to promote
- `key_from`: Field in array items to use as keys
- `value_from`: Field in array items to use as values

**Example:**
```json
{
  "op": "promote_list_to_keys",
  "path": "config",
  "child_list": "settings",
  "key_from": "name",
  "value_from": "value"
}
```

#### truncate_list
Limit list size and provide truncation metadata.

**Schema:**
```json
{
  "op": "truncate_list",
  "path": "string",
  "max_size": "number",
  "shape": {
    "data": "$truncated",
    "metadata": {
      "truncated": "$was_truncated",
      "original_count": "$original_count"
    }
  }
}
```

**Parameters:**
- `path`: Path to the array to truncate
- `max_size`: Maximum number of items to keep
- `shape`: Output structure with special variables

**Special Variables:**
- `$truncated`: The truncated array
- `$was_truncated`: Boolean indicating if truncation occurred
- `$original_count`: Original array length

**Example:**
```json
{
  "op": "truncate_list",
  "path": "results",
  "max_size": 10,
  "shape": {
    "items": "$truncated",
    "meta": {
      "has_more": "$was_truncated",
      "total": "$original_count"
    }
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
      "path": "orders",
      "shape": {
        "min_amount": "$min(amount)",
        "max_amount": "$max(amount)",
        "analytics_note": "Limited aggregation functions available"
      }
    }
  ],
  "output": {
    "orders": "$working.orders",
    "analytics": "$working.orders"
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
- **v0.2.0**: Added transform, copy, move, and merge operations with comprehensive array support
- **Future**: Additional aggregation functions, conditional operations, custom functions