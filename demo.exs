#!/usr/bin/env elixir

# Demo script for DslInterpreter
# Run with: mix run demo.exs

defmodule DslDemo do
  def run do
    IO.puts("🔧 DSL Interpreter Demo")
    IO.puts("=" |> String.duplicate(40))

    # Demo 1: Basic transformation
    IO.puts("\n📊 Demo 1: Basic Data Transformation")
    demo_basic_transformation()

    # Demo 2: Complex nested operations
    IO.puts("\n🎯 Demo 2: Complex Nested Operations")
    demo_complex_operations()

    # Demo 3: List operations
    IO.puts("\n📝 Demo 3: List Transformations")
    demo_list_operations()

    IO.puts("\n✅ All demos completed successfully!")
  end

  defp demo_basic_transformation do
    data = %{
      "user_info" => %{
        "name" => "Alice Johnson",
        "email" => "alice@example.com",
        "age" => 30,
        "internal_id" => "usr_12345"  # Should be removed
      },
      "metadata" => %{
        "source" => "registration_api",
        "timestamp" => "2023-10-15T10:30:00Z"
      }
    }

    dsl = %{
      "root" => %{"path" => "user_info"},
      "pipeline" => [
        %{"op" => "drop", "paths" => ["internal_id"]}
      ],
      "output" => %{
        "user" => "$working",
        "source" => "$root.metadata.source",
        "processed_at" => "$root.metadata.timestamp"
      }
    }

    result = DslInterpreter.execute(data, dsl)
    IO.puts("Input data: #{inspect(data, pretty: true)}")
    IO.puts("Result: #{inspect(result, pretty: true)}")
  end

  defp demo_complex_operations do
    data = %{
      "security_events" => [
        %{
          "id" => "evt-001",
          "severity" => "high",
          "network" => %{"sourceIp" => "192.168.1.100"},
          "detectionFields" => [
            %{"key" => "malware_family", "value" => "Trojan.Generic"},
            %{"key" => "confidence", "value" => "0.95"}
          ]
        },
        %{
          "id" => "evt-002",
          "severity" => "medium",
          "network" => %{"sourceIp" => "10.0.0.5"},
          "detectionFields" => [
            %{"key" => "attack_type", "value" => "SQL Injection"},
            %{"key" => "confidence", "value" => "0.80"}
          ]
        }
      ]
    }

    dsl = %{
      "root" => %{"path" => ""},
      "pipeline" => [
        # Convert detection fields to maps
        %{
          "op" => "list_to_map",
          "path" => "security_events[].detectionFields",
          "key_from" => "key",
          "value_from" => "value"
        },
        # Remove network info for privacy
        %{"op" => "drop", "paths" => ["security_events[].network"]}
      ],
      "output" => %{
        "processed_events" => "$working.security_events"
      }
    }

    result = DslInterpreter.execute(data, dsl)
    IO.puts("Original events: #{length(data["security_events"])} events")
    IO.puts("Processed result: #{inspect(result, pretty: true)}")
  end

  defp demo_list_operations do
    data = %{
      "large_dataset" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    }

    dsl = %{
      "root" => %{"path" => ""},
      "pipeline" => [
        %{
          "op" => "truncate_list",
          "path" => "large_dataset",
          "max_size" => 5,
          "shape" => %{
            "total_items" => "$length",
            "sample" => "$slice(0, 5)",
            "status" => "truncated"
          }
        }
      ],
      "output" => %{
        "summary" => "$working.large_dataset"
      }
    }

    result = DslInterpreter.execute(data, dsl)
    IO.puts("Original list size: #{length(data["large_dataset"])}")
    IO.puts("Truncated result: #{inspect(result, pretty: true)}")
  end
end

# Run the demo
DslDemo.run()
