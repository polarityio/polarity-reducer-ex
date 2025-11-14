# PolarityReducerEx

A powerful Elixir-based JSON DSL interpreter for data transformation and payload reduction. This library provides a declarative way to transform complex nested data structures using a simple JSON configuration format.

## Features

- 🔧 **Declarative transformations**: Define data transformations using JSON configuration
- 🎯 **Path-based operations**: Support for dot notation and array wildcards (`[]`)
- 🔄 **Pipeline processing**: Chain multiple operations sequentially
- 🌟 **Special variables**: `$root`, `$working`, `$length`, `$slice()`, `$min()`, `$max()`
- 🧹 **Data cleanup**: Automatic pruning of empty values
- 📊 **Aggregations**: Built-in min/max aggregation functions
- 🚀 **Production ready**: Comprehensive error handling and testing

## Core Concepts

### DSL Structure
Every DSL configuration has three main sections:

1. **`root`**: Defines the initial working object
2. **`pipeline`**: Array of transformation operations
3. **`output`**: Final projection with variable resolution

### Quick Example

```elixir
data = %{
  "user_data" => %{"name" => "Alice", "email" => "alice@example.com"},
  "metadata" => %{"source" => "api"}
}

dsl = %{
  "root" => %{"path" => "user_data"},
  "pipeline" => [],
  "output" => %{
    "user_name" => "$working.name",
    "source_info" => "$root.metadata.source"
  }
}

result = DslInterpreter.execute(data, dsl)
# => %{"user_name" => "Alice", "source_info" => "api"}
```

## Supported Operations

| Operation | Description | Example Use Case |
|-----------|-------------|------------------|
| `drop` | Remove specific paths | Remove sensitive data |
| `project` | Create new structure with mapping | Reshape API responses |
| `project_and_replace` | Replace entire working object | Complete restructuring |
| `hoist_map_values` | Move nested values to parent | Flatten configuration |
| `list_to_map` | Convert key-value list to map | Transform settings arrays |
| `list_to_dynamic_map` | Group list items by key | Organize by categories |
| `promote_list_to_keys` | Convert list to parent keys | Extract metadata |
| `truncate_list` | Limit list size with metadata | Handle large datasets |
| `aggregate_list` | Compute min/max aggregations | Summarize metrics |
| `prune` | Remove empty values recursively | Clean up sparse data |

## Installation

Add `polarity_reducer_ex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:polarity_reducer_ex, "~> 0.1.0"}
  ]
end
```

## Usage

### Basic Transformation

```elixir
# Transform security event data
data = %{
  "events" => [
    %{"id" => 1, "severity" => "high", "details" => %{"malware" => "trojan"}},
    %{"id" => 2, "severity" => "low", "details" => %{"malware" => "adware"}}
  ]
}

dsl = %{
  "root" => %{"path" => ""},
  "pipeline" => [
    %{"op" => "drop", "paths" => ["events[].details"]}
  ],
  "output" => %{"clean_events" => "$working.events"}
}

DslInterpreter.execute(data, dsl)
```

### Advanced Operations

```elixir
# Convert configuration list to map
dsl = %{
  "root" => %{"path" => "config"},
  "pipeline" => [
    %{
      "op" => "list_to_map",
      "path" => "settings",
      "key_from" => "name",
      "value_from" => "value"
    }
  ],
  "output" => %{"config_map" => "$working.settings"}
}
```

### Wildcard Path Support

The DSL supports powerful path traversal with `[]` wildcards:

- `"events[].network"` - Access network field in all events
- `"data[].users[].profile"` - Nested array access
- `"config.servers[].status"` - Mixed object/array paths

## Documentation

For detailed examples and advanced usage patterns, see:
- [EXAMPLES.md](EXAMPLES.md) - Comprehensive examples
- [API Documentation](https://hexdocs.pm/polarity_reducer_ex) - Full API reference

## Testing

Run the test suite:

```bash
mix test
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Add tests for your changes
4. Ensure all tests pass (`mix test`)
5. Commit your changes (`git commit -am 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

