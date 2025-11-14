# DslInterpreter Examples

This file contains comprehensive examples of how to use the DslInterpreter module.

## Example 1: Basic Data Transformation

```elixir
# Sample security event data
data = %{
  "integrationName" => "SecurityTool",
  "timestamp" => "2023-10-15T10:30:00Z",
  "details" => %{
    "events" => [
      %{
        "id" => "evt-001",
        "severity" => "high",
        "network" => %{"sourceIp" => "192.168.1.100", "destIp" => "10.0.0.5"},
        "securityResult" => [
          %{
            "detectionFields" => [
              %{"key" => "malware_family", "value" => "Trojan.Generic"},
              %{"key" => "confidence", "value" => "0.95"}
            ]
          }
        ],
        "metadata" => %{"processed" => true, "internal" => "debug_info"}
      }
    ]
  },
  "summary" => %{"total_events" => 1, "high_severity" => 1}
}

# DSL configuration
dsl = %{
  "root" => %{
    "path" => "details",
    "on_null" => "return_empty"
  },
  "pipeline" => [
    # Remove internal debug information
    %{
      "op" => "drop",
      "paths" => ["events[].metadata.internal"]
    },
    # Convert detection fields from list to map
    %{
      "op" => "list_to_map",
      "path" => "events[].securityResult[].detectionFields",
      "key_from" => "key",
      "value_from" => "value"
    },
    # Project only the fields we need
    %{
      "op" => "project_and_replace",
      "projection" => %{
        "security_events" => "events",
        "processed_count" => "events"
      }
    }
  ],
  "output" => %{
    "source" => "$root.integrationName",
    "timestamp" => "$root.timestamp",
    "summary" => "$root.summary",
    "processed_data" => "$working"
  }
}

result = DslInterpreter.execute(data, dsl)
```

## Example 2: List Aggregation and Truncation

```elixir
# Large dataset example
data = %{
  "scan_results" => %{
    "assets" => [
      %{"hostname" => "server1", "last_seen" => "2023-10-15", "vulnerabilities" => 5},
      %{"hostname" => "server2", "last_seen" => "2023-10-14", "vulnerabilities" => 2},
      %{"hostname" => "server3", "last_seen" => "2023-10-16", "vulnerabilities" => 8},
      # ... many more assets
    ]
  }
}

dsl = %{
  "root" => %{"path" => "scan_results"},
  "pipeline" => [
    # Truncate the asset list but preserve metadata
    %{
      "op" => "truncate_list",
      "path" => "assets",
      "max_size" => 10,
      "shape" => %{
        "total_assets" => "$length",
        "sample_assets" => "$slice(0, 10)",
        "hostnames_sample" => "$map_slice(0, 5, hostname)"
      }
    }
  ],
  "output" => %{
    "asset_summary" => "$working.assets"
  }
}
```

## Example 3: Complex Nested Transformations

```elixir
# IoC (Indicator of Compromise) data transformation
data = %{
  "threat_intel" => %{
    "iocSources" => [
      %{
        "source" => "feed1",
        "addresses" => [
          %{"type" => "ipv4", "address" => "1.2.3.4"},
          %{"type" => "domain", "address" => "malicious.com"},
          %{"type" => "ipv4", "address" => "5.6.7.8"}
        ]
      }
    ]
  }
}

dsl = %{
  "root" => %{"path" => "threat_intel"},
  "pipeline" => [
    # Group addresses by type dynamically
    %{
      "op" => "list_to_dynamic_map",
      "path" => "iocSources[].addresses",
      "key_from" => "type",
      "value_from" => "address"
    }
  ],
  "output" => %{
    "organized_iocs" => "$working"
  }
}
```

## Example 4: Data Cleanup with Pruning

```elixir
# Messy data that needs cleanup
data = %{
  "dirty_data" => %{
    "valid_field" => "keep this",
    "empty_string" => "",
    "null_field" => nil,
    "empty_object" => %{},
    "empty_array" => [],
    "nested" => %{
      "keep_me" => "important",
      "remove_me" => "",
      "deep_nested" => %{
        "also_empty" => nil,
        "valuable" => "data"
      }
    }
  }
}

dsl = %{
  "root" => %{"path" => "dirty_data"},
  "pipeline" => [
    %{
      "op" => "prune",
      "strategy" => "empty_values"
    }
  ],
  "output" => %{
    "cleaned" => "$working"
  }
}

# Result will only contain non-empty values
```

## Key Features Demonstrated

1. **Path Traversal**: Supports dot notation and array wildcards (`[]`)
2. **Root Object Handling**: Configurable behavior for null paths
3. **Pipeline Operations**: Sequential transformation operations
4. **Special Variables**: `$root`, `$working`, `$length`, `$slice()`, etc.
5. **Data Cleanup**: Automatic pruning of empty values
6. **Flexible Output**: Mix original and transformed data in final result

## Supported Operations

- `drop`: Remove specific paths
- `project`: Create new structure with mapping
- `project_and_replace`: Replace entire working object
- `hoist_map_values`: Move nested map values to parent
- `list_to_map`: Convert key-value list to map
- `list_to_dynamic_map`: Group list items by key
- `promote_list_to_keys`: Convert list to parent keys
- `truncate_list`: Limit list size with metadata
- `aggregate_list`: Compute min/max aggregations
- `prune`: Remove empty values recursively