# DSL Pipeline Testing SDK

A comprehensive SDK for testing, debugging, and analyzing DSL pipelines with step-by-step execution visualization.

## Overview

The `DslPipelineTester` module provides developers with powerful tools to:

- **Step-by-step execution**: See exactly how data transforms at each pipeline stage
- **Performance profiling**: Measure execution time for each operation
- **Visual debugging**: Compare before/after states for each transformation
- **Pipeline analysis**: Get insights about complexity, potential issues, and optimization recommendations
- **File-based testing**: Load test data and pipelines from JSON files
- **Error handling**: Gracefully handle and report configuration and execution errors

## Quick Start

### Basic Usage

```elixir
# Test a pipeline with JSON files
DslPipelineTester.test_pipeline("examples/data.json", "examples/pipeline.json")

# Test directly with data structures
data = %{"events" => [%{"id" => 1, "name" => "test"}]}
pipeline = %{
  "root" => %{"path" => ""},
  "pipeline" => [%{"op" => "drop", "paths" => ["events[].id"]}],
  "output" => %{"result" => "$working"}
}

DslPipelineTester.test_pipeline_direct(data, pipeline)
```

### Analyze Pipeline Complexity

```elixir
DslPipelineTester.analyze_pipeline(pipeline_config)
```

## API Reference

### Core Functions

#### `test_pipeline/2`

Test a pipeline using JSON files for data and configuration.

**Parameters:**
- `data_file_path`: Path to JSON file containing example data
- `pipeline_file_path`: Path to JSON file containing DSL configuration

**Returns:** Detailed test report with step-by-step execution data

**Example:**
```elixir
result = DslPipelineTester.test_pipeline("data.json", "pipeline.json")
# Returns: %{success: true, steps: [...], summary: %{...}, ...}
```

#### `test_pipeline_direct/3`

Test a pipeline directly with provided data and configuration.

**Parameters:**
- `data`: The input data map
- `dsl_config`: The DSL configuration map  
- `opts`: Options (optional)
  - `:verbose` - Show detailed output (default: true)
  - `:show_diffs` - Show before/after differences (default: true)

**Returns:** Detailed test result with execution metadata

**Example:**
```elixir
data = %{"users" => [%{"name" => "Alice", "age" => 30}]}
config = %{
  "root" => %{"path" => ""},
  "pipeline" => [%{"op" => "drop", "paths" => ["users[].age"]}],
  "output" => %{"result" => "$working"}
}

result = DslPipelineTester.test_pipeline_direct(data, config, verbose: false)
```

#### `analyze_pipeline/1`

Analyze a pipeline configuration for complexity, potential issues, and optimization opportunities.

**Parameters:**
- `dsl_config`: The DSL configuration to analyze

**Returns:** Analysis report with insights and recommendations

**Example:**
```elixir
analysis = DslPipelineTester.analyze_pipeline(dsl_config)
# Returns: %{
#   total_operations: 5,
#   complexity_score: 12,
#   operation_types: %{"drop" => 2, "project" => 1},
#   potential_issues: [...],
#   recommendations: [...]
# }
```

#### `load_pipeline_config/1`

Load and validate a DSL configuration from a JSON file.

**Parameters:**
- `file_path`: Path to the JSON configuration file

**Returns:** 
- `{:ok, dsl_config}` on success
- `{:error, reason}` on failure

**Example:**
```elixir
case DslPipelineTester.load_pipeline_config("pipeline.json") do
  {:ok, config} -> IO.puts("Configuration loaded successfully")
  {:error, reason} -> IO.puts("Failed to load: #{reason}")
end
```

## Test Report Structure

When you run `test_pipeline/2` or `test_pipeline_direct/3`, you get a comprehensive report:

```elixir
%{
  success: true,                    # Whether execution succeeded
  original_data: %{...},           # Original input data
  final_result: %{...},            # Final transformed result
  total_execution_time_ms: 42,     # Total execution time
  steps: [                         # Step-by-step execution data
    %{
      step: 0,
      operation: "root_resolution",
      description: "Using entire input as working object",
      input: %{...},
      output: %{...},
      execution_time_ms: 0
    },
    %{
      step: 1,
      operation: "drop",
      description: "Dropping 2 paths: events[].id, events[].debug",
      input: %{...},
      output: %{...},
      execution_time_ms: 1,
      operation_config: %{"op" => "drop", "paths" => [...]}
    },
    # ... more steps
  ],
  summary: %{
    total_steps: 4,
    operations_executed: 2,
    data_size_reduction: "+15.3%"   # Positive = growth, negative = reduction
  }
}
```

## Pipeline Analysis Features

The `analyze_pipeline/1` function provides detailed insights:

### Complexity Scoring

Operations are weighted by complexity:
- **drop**: 1 point (simple)
- **project**: 2 points (moderate)  
- **list_to_map**: 3 points (complex)
- **aggregate_list**: 4 points (most complex)

### Potential Issues Detection

- **Long pipelines**: Warns about pipelines with >10 operations
- **Consecutive prune operations**: Detects inefficient duplicate pruning
- **Missing error handling**: Identifies operations that might fail silently

### Optimization Recommendations

- **Operation ordering**: Suggests optimal operation sequences
- **Performance tips**: Recommends placing expensive operations strategically
- **Best practices**: Highlights common patterns and anti-patterns

## JSON File Format

### Data File Format
Any valid JSON structure. Example:
```json
{
  "events": [
    {"id": 1, "name": "Event 1", "data": {"value": 42}},
    {"id": 2, "name": "Event 2", "data": {"value": 24}}
  ],
  "metadata": {"source": "api", "timestamp": "2023-10-15T10:30:00Z"}
}
```

### Pipeline File Format
Standard DSL configuration:
```json
{
  "root": {
    "path": "events",
    "on_null": "return_empty"
  },
  "pipeline": [
    {"op": "drop", "paths": ["[].id"]},
    {"op": "project", "path": "", "mapping": {"clean_events": "events"}}
  ],
  "output": {
    "source": "$root.metadata.source",
    "events": "$working.clean_events"
  }
}
```

## Error Handling

The SDK provides comprehensive error handling:

### File Errors
- **Missing files**: Clear error messages for non-existent files
- **JSON parsing errors**: Detailed syntax error reporting
- **Permission errors**: Helpful messages for access issues

### Configuration Errors  
- **Missing required keys**: Identifies which DSL keys are missing
- **Invalid operation configs**: Validates operation parameters
- **Type mismatches**: Catches incorrect data types

### Execution Errors
- **Operation failures**: Graceful handling of runtime errors
- **Path resolution errors**: Clear messages for invalid paths
- **Data type errors**: Helpful debugging for type mismatches

## Performance Features

### Execution Timing
- **Per-operation timing**: See which operations are slowest
- **Total execution time**: Overall pipeline performance
- **Bottleneck identification**: Find the slowest operations

### Memory Usage Analysis
- **Data size tracking**: Monitor data growth/reduction through pipeline
- **Memory efficiency**: Identify operations that significantly change data size
- **Optimization opportunities**: Suggest ways to reduce memory usage

## Best Practices

### Testing Strategy
1. **Start simple**: Begin with minimal test cases
2. **Test edge cases**: Include empty data, null values, malformed input
3. **Performance testing**: Use large datasets to identify bottlenecks
4. **Error scenarios**: Test with invalid configurations and data

### Pipeline Development
1. **Incremental building**: Add operations one at a time and test
2. **Use analysis**: Run `analyze_pipeline/1` regularly during development
3. **Optimize ordering**: Place expensive operations strategically
4. **Document intentions**: Use clear operation descriptions

### Debugging Workflow
1. **Enable verbose output**: Use detailed reporting during development
2. **Check intermediate steps**: Examine data at each pipeline stage
3. **Use small datasets**: Debug with minimal test data first
4. **Validate assumptions**: Verify data structure expectations

## Examples

See the `examples/` directory for comprehensive examples:
- `security_events.json` + `security_pipeline.json`: Security data transformation
- `user_data.json` + `user_pipeline.json`: User profile processing
- `sdk_demo.exs`: Complete SDK demonstration script

## Integration

### With Test Suites
```elixir
defmodule MyPipelineTest do
  use ExUnit.Case
  
  test "security event processing pipeline" do
    result = DslPipelineTester.test_pipeline_direct(
      load_test_data(),
      load_pipeline_config(),
      verbose: false
    )
    
    assert result.success
    assert length(result.steps) == 5
    assert result.final_result["events"] != nil
  end
end
```

### With Development Workflow
```elixir
# In your development script
data = load_production_sample()
pipeline = load_current_pipeline()

# Quick validation
result = DslPipelineTester.test_pipeline_direct(data, pipeline)
if not result.success do
  IO.puts("Pipeline failed: #{result.error}")
  System.halt(1)
end

# Performance check
if result.total_execution_time_ms > 100 do
  IO.puts("Warning: Pipeline is slow (#{result.total_execution_time_ms}ms)")
end
```

This SDK provides everything you need to develop, test, and optimize DSL pipelines with confidence.