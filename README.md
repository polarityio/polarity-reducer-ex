# Polarity Reducer Ex

A powerful Elixir-based DSL (Domain Specific Language) interpreter for data transformation pipelines with comprehensive web interface, testing framework, and Docker deployment capabilities.

## Overview

Polarity Reducer Ex enables you to define complex data transformation pipelines using a simple JSON-based DSL. It supports a wide range of operations including field manipulation, data projection, pruning, renaming, and advanced date/time operations. The system includes a comprehensive testing framework for pipeline validation and performance analysis.

## Features

- **Complete DSL Operations**: drop, project, rename, prune, date/time operations, and more
- **Pipeline Testing Framework**: Comprehensive validation, analysis, and performance monitoring
- **Web Interface**: JSON editor with syntax highlighting and multiple view modes  
- **Docker Deployment**: Production-ready deployment with Caddy reverse proxy
- **Date/Time Support**: Comprehensive date parsing, formatting, and arithmetic
- **Array Processing**: Deep nested array and object transformations
- **Performance Optimized**: Handles large datasets efficiently with detailed metrics
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

#### Basic Pipeline Execution

```bash
# Start the interactive shell
iex -S mix

# Execute a DSL transformation
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

#### Pipeline Testing and Validation

```bash
# Test a pipeline with detailed analysis
iex -S mix

# Load test data
iex> data = %{
...>   "events" => [
...>     %{"id" => 1, "name" => "Event 1", "date" => "2024-01-01"},
...>     %{"id" => 2, "name" => "Event 2", "date" => "2024-01-02"}
...>   ]
...> }

# Define a DSL configuration
iex> dsl_config = %{
...>   "version" => "1.0",
...>   "root" => %{"path" => ""},
...>   "pipeline" => [
...>     %{"op" => "drop", "paths" => ["events[].id"]},
...>     %{"op" => "format_date", "path" => "events[].date", "format" => "human"}
...>   ],
...>   "output" => %{"result" => "$working.events"}
...> }

# Validate the pipeline configuration
iex> PolarityReducerEx.DslPipelineTester.validate_pipeline_public(dsl_config)
:ok

# Test the pipeline with detailed step tracking
iex> result = PolarityReducerEx.DslPipelineTester.test_pipeline_direct(data, dsl_config, verbose: true)
iex> result.success
true
iex> length(result.steps)
4
iex> result.total_execution_time_ms
15

# Analyze pipeline complexity and get recommendations
iex> analysis = PolarityReducerEx.DslPipelineTester.analyze_pipeline(dsl_config)
iex> analysis.total_operations
2
iex> analysis.complexity_score
5.0
iex> analysis.recommendations
["Consider combining drop operations with prune operations for better performance"]
```

#### Loading and Testing Pipeline Configurations from Files

```bash
# Create a pipeline configuration file
cat > pipeline_config.json << 'EOF'
{
  "version": "1.0",
  "root": {"path": "data"},
  "pipeline": [
    {"op": "drop", "paths": ["internal_id"]},
    {"op": "rename", "mapping": {"user_name": "name"}},
    {"op": "prune", "strategy": "empty_values"}
  ],
  "output": {"result": "$working"}
}
EOF

# Test from file in IEx
iex -S mix
iex> {:ok, config} = PolarityReducerEx.DslPipelineTester.load_pipeline_config("pipeline_config.json")
iex> test_data = %{"data" => %{"user_name" => "John", "internal_id" => 123, "email" => ""}}
iex> result = PolarityReducerEx.DslPipelineTester.test_pipeline_direct(test_data, config, verbose: true)
```

#### Command Line Testing Scripts

Create testing scripts for batch pipeline validation:

```bash
# Create a test script
cat > test_pipeline.exs << 'EOF'
# Load the application
Mix.install([
  {:jason, "~> 1.4"},
  {:polarity_reducer_ex, path: "."}
])

# Test data
data = %{
  "users" => [
    %{"id" => 1, "first_name" => "John", "last_name" => "Doe", "email" => "john@example.com"},
    %{"id" => 2, "first_name" => "Jane", "last_name" => "Smith", "email" => "jane@example.com"}
  ]
}

# DSL configuration
dsl_config = %{
  "version" => "1.0",
  "root" => %{"path" => ""},
  "pipeline" => [
    %{"op" => "rename", "mapping" => %{
      "users[].first_name" => "users[].firstName",
      "users[].last_name" => "users[].lastName"
    }},
    %{"op" => "drop", "paths" => ["users[].id"]},
    %{"op" => "current_timestamp", "path" => "processed_at", "format" => "iso8601"}
  ],
  "output" => %{"users" => "$working.users", "timestamp" => "$working.processed_at"}
}

# Validate pipeline
case PolarityReducerEx.DslPipelineTester.validate_pipeline_public(dsl_config) do
  :ok -> IO.puts("✓ Pipeline validation passed")
  {:error, msg} -> IO.puts("✗ Pipeline validation failed: #{msg}")
end

# Test pipeline execution
result = PolarityReducerEx.DslPipelineTester.test_pipeline_direct(data, dsl_config, verbose: false)

if result.success do
  IO.puts("✓ Pipeline execution successful")
  IO.puts("  Operations executed: #{result.summary.operations_executed}")
  IO.puts("  Total execution time: #{result.total_execution_time_ms}ms")
  IO.puts("  Data size change: #{result.summary.data_size_reduction}")
  
  # Print final result
  IO.puts("\nFinal result:")
  IO.inspect(result.final_result, pretty: true)
else
  IO.puts("✗ Pipeline execution failed: #{result.error}")
end

# Analyze pipeline
analysis = PolarityReducerEx.DslPipelineTester.analyze_pipeline(dsl_config)
IO.puts("\nPipeline Analysis:")
IO.puts("  Complexity score: #{analysis.complexity_score}")
IO.puts("  Total operations: #{analysis.total_operations}")

if length(analysis.potential_issues) > 0 do
  IO.puts("  Potential issues:")
  Enum.each(analysis.potential_issues, fn issue ->
    IO.puts("    - #{issue}")
  end)
end

if length(analysis.recommendations) > 0 do
  IO.puts("  Recommendations:")
  Enum.each(analysis.recommendations, fn rec ->
    IO.puts("    - #{rec}")
  end)
end
EOF

# Run the test script
elixir test_pipeline.exs
```

#### Performance Testing

```bash
# Create a performance test script
cat > performance_test.exs << 'EOF'
# Generate large test dataset
large_data = %{
  "records" => Enum.map(1..10000, fn i ->
    %{
      "id" => i,
      "name" => "Record #{i}",
      "value" => :rand.uniform(1000),
      "created_at" => "2024-01-#{rem(i, 28) + 1}T10:00:00Z",
      "metadata" => %{"debug" => "remove_me", "important" => "keep_me"}
    }
  end)
}

# Complex DSL pipeline
dsl_config = %{
  "version" => "1.0",
  "root" => %{"path" => ""},
  "pipeline" => [
    %{"op" => "drop", "paths" => ["records[].metadata.debug"]},
    %{"op" => "format_date", "path" => "records[].created_at", "format" => "human"},
    %{"op" => "prune", "strategy" => "empty_values"},
    %{"op" => "current_timestamp", "path" => "processed_at", "format" => "unix"}
  ],
  "output" => %{"data" => "$working.records", "processed_at" => "$working.processed_at"}
}

IO.puts("Testing pipeline performance with #{length(large_data["records"])} records...")

result = PolarityReducerEx.DslPipelineTester.test_pipeline_direct(large_data, dsl_config, verbose: false)

if result.success do
  IO.puts("✓ Performance test completed successfully")
  IO.puts("  Total execution time: #{result.total_execution_time_ms}ms")
  IO.puts("  Average time per operation: #{div(result.total_execution_time_ms, result.summary.operations_executed)}ms")
  IO.puts("  Records processed: #{length(result.final_result["data"])}")
  IO.puts("  Data size change: #{result.summary.data_size_reduction}")
else
  IO.puts("✗ Performance test failed: #{result.error}")
end
EOF

# Run performance test
elixir performance_test.exs
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

## Testing Framework

The system includes a comprehensive testing framework for pipeline validation and analysis:

### Pipeline Validation

```elixir
# Validate DSL configuration syntax
PolarityReducerEx.DslPipelineTester.validate_pipeline_public(dsl_config)

# Validate complete pipeline with input/output
PolarityReducerEx.DslPipelineTester.validate_full_object(input_data, dsl_config, expected_output)
```

### Pipeline Testing

```elixir
# Test pipeline execution with detailed step tracking
result = PolarityReducerEx.DslPipelineTester.test_pipeline_direct(data, dsl_config, verbose: true)

# Access detailed results
result.success          # Boolean success status
result.steps           # Array of execution steps with timing
result.final_result    # Final transformed data
result.total_execution_time_ms  # Total execution time
result.summary         # Execution summary with metrics
```

### Pipeline Analysis

```elixir
# Analyze pipeline complexity and performance characteristics
analysis = PolarityReducerEx.DslPipelineTester.analyze_pipeline(dsl_config)

analysis.total_operations     # Number of operations
analysis.complexity_score     # Complexity rating
analysis.operation_types      # Count by operation type
analysis.potential_issues     # Array of potential problems
analysis.recommendations      # Array of optimization suggestions
```

### Configuration Management

```elixir
# Load pipeline configuration from JSON file
{:ok, config} = PolarityReducerEx.DslPipelineTester.load_pipeline_config("path/to/config.json")

# Handle errors gracefully
case PolarityReducerEx.DslPipelineTester.load_pipeline_config("invalid.json") do
  {:ok, config} -> # Process config
  {:error, reason} -> # Handle error
end
```

## DSL Specification

### Structure

A DSL configuration consists of three main sections:

```json
{
  "version": "1.0",
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

### Running Tests

```bash
# Run all tests
mix test

# Run specific test suites  
mix test test/dsl_operations_test.exs
mix test test/dsl_integration_test.exs
mix test test/dsl_pipeline_tester_test.exs

# Run performance tests
mix test --include performance

# Run stress tests
mix test --include stress

# Run with coverage
mix test --cover

# Run tests with detailed output
mix test --trace
```

### Test Suites

The project includes comprehensive test coverage:

- **Unit tests**: Individual operation testing (`dsl_operations_test.exs`)
- **Integration tests**: Complex pipeline scenarios (`dsl_integration_test.exs`)
- **Pipeline testing framework**: Testing and validation framework (`dsl_pipeline_tester_test.exs`)
- **Performance tests**: Large dataset processing
- **Stress tests**: Memory and concurrency testing

### Custom Test Data

Create custom test scenarios:

```bash
# Generate test data for specific scenarios
mix test test/dsl_integration_test.exs --include custom_data
```

## Architecture

### Components

1. **DslInterpreter**: Core transformation engine
2. **DslPipelineTester**: Testing and validation framework
3. **WebServer**: HTTP API and static file serving
4. **Application**: OTP supervisor managing server lifecycle

### Design Principles

- **Immutable transformations**: All operations return new data structures
- **Pipeline architecture**: Sequential operation processing
- **Error isolation**: Operations fail gracefully without affecting others
- **Extensible design**: Easy to add new operations
- **Comprehensive testing**: Built-in validation and performance monitoring

### File Structure

```
polarity_reducer_ex/
├── lib/
│   ├── dsl_interpreter.ex              # Core DSL engine
│   ├── dsl_pipeline_tester.ex          # Testing and validation framework
│   ├── web_server.ex                   # HTTP server and API
│   └── polarity_reducer_ex.ex          # Application supervisor
├── priv/
│   └── static/                         # Web interface assets
├── test/
│   ├── dsl_operations_test.exs         # Individual operation tests
│   ├── dsl_integration_test.exs        # Complex pipeline tests
│   ├── dsl_pipeline_tester_test.exs    # Testing framework tests
│   ├── dsl_performance_test.exs        # Performance benchmarks
│   └── polarity_reducer_ex_test.exs    # Basic functionality tests
├── docker-compose.yml                  # Docker deployment config
├── Dockerfile                          # Container build instructions
└── mix.exs                             # Project configuration
```

## Error Handling

The system provides comprehensive error handling:

- **Invalid operations**: Clear error messages for unsupported operations
- **Path resolution errors**: Graceful handling of invalid paths
- **Type mismatches**: Informative errors for incorrect data types
- **Date parsing errors**: Detailed feedback for invalid date formats
- **Memory limits**: Protection against excessive memory usage
- **Pipeline validation**: Pre-execution validation of DSL configurations
- **Step-by-step error tracking**: Detailed error reporting for failed operations

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

### v0.2.0
- Added comprehensive pipeline testing framework (`DslPipelineTester`)
- Pipeline validation and configuration loading from JSON files
- Performance monitoring with execution timing and data size tracking
- Pipeline complexity analysis with optimization recommendations
- Step-by-step execution tracking with detailed error reporting
- Command-line testing utilities and batch validation scripts

### v0.1.0 
- Initial release with core DSL operations
- Web interface with JSON editor
- Docker deployment with Caddy
- Comprehensive date/time operations
- Performance optimizations
- Full test suite with benchmarks
