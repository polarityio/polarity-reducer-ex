# Polarity Reducer Ex

A powerful Elixir-based DSL (Domain Specific Language) interpreter for data transformation pipelines with comprehensive web interface and Docker deployment capabilities.

## Overview

Polarity Reducer Ex enables you to define complex data transformation pipelines using a simple JSON-based DSL. It supports a wide range of operations including field manipulation, data projection, pruning, renaming, and advanced date/time operations.

## Features

- **Complete DSL Operations**: drop, project, rename, prune, date/time operations, and more
- **Web Interface**: JSON editor with syntax highlighting and multiple view modes  
- **Docker Deployment**: Production-ready deployment with Caddy reverse proxy
- **Date/Time Support**: Comprehensive date parsing, formatting, and arithmetic
- **Array Processing**: Deep nested array and object transformations
- **Performance Optimized**: Handles large datasets efficiently
- **Type Safety**: Built with Elixir's robust type system and pattern matching

## Installation

### Prerequisites

- Elixir 1.18+ and OTP 27+
- Docker and Docker Compose (for containerized deployment)
- Mix build tool

### Local Development

```bash
# Clone the repository
git clone <repository-url>
cd polarity_reducer_ex

# Install dependencies
mix deps.get

# Compile the application
mix compile

# Run tests
mix test
```

### Docker Deployment

```bash
# Build and start with Docker Compose
docker-compose up --build

# The web interface will be available at:
# http://localhost:8080 (HTTP)
# https://localhost:8443 (HTTPS - with automatic SSL certificates)
```

## Usage

### Command Line Interface

```bash
# Execute a DSL transformation
iex -S mix
iex> data = %{"users" => [%{"name" => "John", "email" => "john@example.com"}]}
iex> dsl = %{
...>   "root" => %{"path" => ""},
...>   "pipeline" => [
...>     %{"op" => "rename", "mapping" => %{"users[].name" => "users[].full_name"}}
...>   ],
...>   "output" => %{"result" => "$working"}
...> }
iex> DslInterpreter.execute(data, dsl)
```

### Web Interface

1. Navigate to `http://localhost:8080` after starting the server
2. Use the JSON editor to input your data and DSL configuration
3. Click "Execute Pipeline" to see the transformation results
4. Switch between tree, code, and view modes for different editing experiences

### REST API

#### Execute Pipeline
```bash
# POST /api/test
curl -X POST http://localhost:4000/api/test \
  -H "Content-Type: application/json" \
  -d '{
    "data": {"users": [{"name": "John"}]},
    "dsl": {
      "root": {"path": ""},
      "pipeline": [
        {"op": "rename", "mapping": {"users[].name": "users[].full_name"}}
      ],
      "output": {"result": "$working"}
    }
  }'
```

#### Health Check
```bash
# GET /health
curl http://localhost:4000/health
```

## DSL Specification

### Structure

A DSL configuration consists of three main sections:

```json
{
  "root": {
    "path": "data.records",
    "on_null": "return_empty"
  },
  "pipeline": [
    {"op": "operation_name", "...": "parameters"}
  ],
  "output": {
    "result": "$working",
    "metadata": "$root.metadata"
  }
}
```

### Root Configuration

- **path**: JSONPath-like expression to select the working dataset
- **on_null**: Behavior when path resolves to null (`"return_empty"` or `"return_original"`)

### Operations

#### drop
Remove fields from the data structure.

```json
{
  "op": "drop",
  "paths": ["users[].internal_id", "metadata.debug"]
}
```

#### rename
Rename fields, supporting nested paths and array indices.

```json
{
  "op": "rename", 
  "mapping": {
    "users[].first_name": "users[].firstName",
    "metadata.created_by": "metadata.creator"
  }
}
```

#### project
Extract and transform specific fields into a new structure.

```json
{
  "op": "project",
  "path": "users",
  "mapping": {
    "id": "user_id",
    "name": "full_name", 
    "contact": "email"
  }
}
```

#### prune
Remove empty, null, or unwanted values.

```json
{
  "op": "prune",
  "strategy": "empty_values"  // or "null_values"
}
```

### Date/Time Operations

#### current_timestamp
Add current timestamp to the data.

```json
{
  "op": "current_timestamp",
  "path": "processed_at",
  "format": "iso8601"  // "iso8601", "unix", "human"
}
```

#### format_date
Format existing date fields.

```json
{
  "op": "format_date",
  "path": "users[].created_at",
  "format": "human"  // "iso8601", "unix", "human", "date_only"
}
```

#### date_add
Add time to a date field.

```json
{
  "op": "date_add",
  "path": "users[].expires_at",
  "amount": 30,
  "unit": "days"  // "seconds", "minutes", "hours", "days", "weeks", "months", "years"
}
```

#### date_diff
Calculate difference between two dates.

```json
{
  "op": "date_diff",
  "from_path": "users[].created_at",
  "to_path": "current_time", 
  "result_path": "users[].account_age_days",
  "unit": "days"
}
```

#### parse_date
Parse date strings into standardized format.

```json
{
  "op": "parse_date",
  "path": "users[].signup_date",
  "output_format": "iso8601"
}
```

### Advanced Operations

#### list_to_map
Convert arrays to maps using specified key fields.

```json
{
  "op": "list_to_map",
  "path": "users",
  "key_field": "id"
}
```

#### aggregate_list
Perform aggregations on array data.

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

### Output Templates

Use variable references in the output section:

- `$working`: Reference the transformed working data
- `$root`: Reference the original input data
- `$working.path.to.field`: Access specific fields
- `$root.original.field`: Access original data fields

```json
{
  "output": {
    "transformed_users": "$working.users",
    "original_metadata": "$root.metadata", 
    "processing_info": {
      "timestamp": "$working.processed_at",
      "count": 42
    }
  }
}
```

## Path Expressions

Path expressions support:

- **Simple paths**: `"user.name"`
- **Array indices**: `"users[0].name"`
- **Array wildcards**: `"users[].name"` (all array elements)
- **Deep nesting**: `"data.users[].profile.settings.theme"`
- **Multiple levels**: `"org.departments[].employees[].projects[].name"`

## Performance

The system is optimized for:

- **Large datasets**: Tested with 10,000+ records
- **Deep nesting**: Efficient handling of deeply nested structures
- **Array processing**: Optimized for large arrays with complex transformations
- **Memory efficiency**: Careful memory management for large data processing
- **Concurrent execution**: Thread-safe operations

Performance benchmarks (typical):
- 5,000 records with complex transformations: ~1.5s
- 25,000 nested objects with field renaming: ~1s
- 6,000 date operations: ~150ms

## Testing

```bash
# Run all tests
mix test

# Run specific test suites  
mix test test/dsl_operations_test.exs
mix test test/dsl_integration_test.exs

# Run performance tests
mix test --include performance

# Run stress tests
mix test --include stress

# Run with coverage
mix test --cover
```

Test suites include:
- **Unit tests**: Individual operation testing
- **Integration tests**: Complex pipeline scenarios  
- **Performance tests**: Large dataset processing
- **Stress tests**: Memory and concurrency testing

## Architecture

### Components

1. **DslInterpreter**: Core transformation engine
2. **WebServer**: HTTP API and static file serving
3. **Application**: OTP supervisor managing server lifecycle

### Design Principles

- **Immutable transformations**: All operations return new data structures
- **Pipeline architecture**: Sequential operation processing
- **Error isolation**: Operations fail gracefully without affecting others
- **Extensible design**: Easy to add new operations

### File Structure

```
polarity_reducer_ex/
├── lib/
│   ├── dsl_interpreter.ex          # Core DSL engine
│   ├── web_server.ex               # HTTP server and API
│   └── polarity_reducer_ex.ex      # Application supervisor
├── priv/
│   └── static/                     # Web interface assets
├── test/
│   ├── dsl_operations_test.exs     # Individual operation tests
│   ├── dsl_integration_test.exs    # Complex pipeline tests
│   ├── dsl_performance_test.exs    # Performance benchmarks
│   └── polarity_reducer_ex_test.exs # Basic functionality tests
├── docker-compose.yml              # Docker deployment config
├── Dockerfile                      # Container build instructions
└── mix.exs                         # Project configuration
```

## Error Handling

The system provides comprehensive error handling:

- **Invalid operations**: Clear error messages for unsupported operations
- **Path resolution errors**: Graceful handling of invalid paths
- **Type mismatches**: Informative errors for incorrect data types
- **Date parsing errors**: Detailed feedback for invalid date formats
- **Memory limits**: Protection against excessive memory usage

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Add tests for new functionality
4. Ensure all tests pass (`mix test`)
5. Update documentation as needed
6. Commit changes (`git commit -m 'Add amazing feature'`)
7. Push to branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Changelog

### v0.1.0 
- Initial release with core DSL operations
- Web interface with JSON editor
- Docker deployment with Caddy
- Comprehensive date/time operations
- Performance optimizations
- Full test suite with benchmarks

