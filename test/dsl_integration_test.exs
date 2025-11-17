defmodule PolarityReducerEx.DslIntegrationTest do
  use ExUnit.Case

  alias PolarityReducerEx.DslInterpreter
  doctest PolarityReducerEx.DslInterpreter

  @moduledoc """
  Integration tests for complex DSL pipeline scenarios.
  Tests combinations of operations and real-world use cases.
  """

  describe "complex pipeline integration" do
    test "user data processing pipeline" do
      # Simulate processing user data from an API response
      data = %{
        "api_response" => %{
          "users" => [
            %{
              "user_id" => "123",
              "first_name" => "John",
              "last_name" => "Doe",
              "email_address" => "john.doe@example.com",
              "created_timestamp" => "2024-01-15T10:30:00Z",
              "last_login_time" => "2024-11-15T14:45:00Z",
              "internal_id" => "abc123",
              "debug_info" => %{"trace_id" => "xyz", "session_id" => "sess1"},
              "preferences" => %{
                "theme" => "dark",
                "notifications" => true,
                "beta_features" => false
              }
            },
            %{
              "user_id" => "456",
              "first_name" => "Jane",
              "last_name" => "Smith",
              "email_address" => "jane.smith@example.com",
              "created_timestamp" => "2024-02-20T08:15:00Z",
              "last_login_time" => "2024-11-14T09:30:00Z",
              "internal_id" => "def456",
              "debug_info" => %{"trace_id" => "abc", "session_id" => "sess2"},
              "preferences" => %{
                "theme" => "light",
                "notifications" => false,
                "beta_features" => true
              }
            }
          ],
          "metadata" => %{
            "request_id" => "req_789",
            "processed_by" => "api_gateway",
            "response_time_ms" => 150
          }
        }
      }

      dsl = %{
        "root" => %{"path" => "api_response"},
        "pipeline" => [
          # Remove sensitive and debug information
          %{"op" => "drop", "paths" => ["users[].internal_id", "users[].debug_info", "metadata.request_id"]},

          # Rename fields to match client expectations
          %{"op" => "rename", "mapping" => %{
            "users[].user_id" => "users[].id",
            "users[].first_name" => "users[].firstName",
            "users[].last_name" => "users[].lastName",
            "users[].email_address" => "users[].email",
            "users[].created_timestamp" => "users[].createdAt",
            "users[].last_login_time" => "users[].lastLoginAt",
            "metadata.processed_by" => "metadata.processor"
          }},

          # Format dates to human readable
          %{"op" => "format_date", "path" => "users[].createdAt", "format" => "human"},
          %{"op" => "format_date", "path" => "users[].lastLoginAt", "format" => "human"},

          # Add processing timestamp
          %{"op" => "current_timestamp", "path" => "metadata.processedAt", "format" => "iso8601"},

          # Clean up empty values
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{
          "users" => "$working.users",
          "metadata" => "$working.metadata",
          "summary" => %{
            "total_users" => 2
          }
        }
      }

      result = DslInterpreter.execute(data, dsl)

      # Verify structure
      assert Map.has_key?(result, "users")
      assert Map.has_key?(result, "summary")
      assert length(result["users"]) == 2

      # Verify first user
      user1 = Enum.at(result["users"], 0)
      assert user1["id"] == "123"
      assert user1["firstName"] == "John"
      assert user1["lastName"] == "Doe"
      assert user1["email"] == "john.doe@example.com"
      assert String.contains?(user1["createdAt"], "2024-01-15")
      assert String.contains?(user1["lastLoginAt"], "2024-11-15")

      # Verify sensitive data was removed
      refute Map.has_key?(user1, "internal_id")
      refute Map.has_key?(user1, "debug_info")

      # Verify summary
      assert result["summary"]["total_users"] == 2

      # Verify metadata is correctly resolved as a map
      assert is_map(result["metadata"])
      assert result["metadata"]["processor"] == "api_gateway"
      assert is_binary(result["metadata"]["processedAt"])
      assert String.contains?(result["metadata"]["processedAt"], "T")
    end

    test "event log processing pipeline" do
      data = %{
        "logs" => [
          %{
            "timestamp" => "1700000000",  # Unix timestamp
            "level" => "INFO",
            "message" => "User login successful",
            "user_id" => "user123",
            "session_id" => "sess_abc",
            "request_id" => "req_xyz",
            "duration_ms" => 250
          },
          %{
            "timestamp" => "1700000300",  # 5 minutes later
            "level" => "ERROR",
            "message" => "Database connection failed",
            "user_id" => "user456",
            "session_id" => "sess_def",
            "request_id" => "req_uvw",
            "duration_ms" => nil,
            "error_code" => "DB_CONN_001"
          },
          %{
            "timestamp" => "1700000600",  # 10 minutes from start
            "level" => "WARN",
            "message" => "High memory usage detected",
            "user_id" => nil,
            "session_id" => nil,
            "request_id" => "req_rst",
            "duration_ms" => 50,
            "memory_usage_mb" => 1024
          }
        ]
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          # Convert Unix timestamps to readable dates
          %{"op" => "format_date", "path" => "logs[].timestamp", "format" => "human"},

          # Rename fields for consistency
          %{"op" => "rename", "mapping" => %{
            "logs[].user_id" => "logs[].userId",
            "logs[].session_id" => "logs[].sessionId",
            "logs[].request_id" => "logs[].requestId",
            "logs[].duration_ms" => "logs[].durationMs"
          }},

          # Add processing metadata
          %{"op" => "current_timestamp", "path" => "processed_at", "format" => "iso8601"},

          # Clean up null values
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{
          "events" => "$working.logs",
          "processed_at" => "$working.processed_at",
          "total_events" => 3
        }
      }

      result = DslInterpreter.execute(data, dsl)

      # Verify events were processed
      assert Map.has_key?(result, "events")
      assert length(result["events"]) == 3

      # Check first event transformation
      event1 = Enum.at(result["events"], 0)
      assert String.contains?(event1["timestamp"], "2023-11-14")  # Converted from Unix
      assert event1["userId"] == "user123"
      assert event1["sessionId"] == "sess_abc"
      assert event1["requestId"] == "req_xyz"
      assert event1["durationMs"] == 250

      # Check error event
      event2 = Enum.at(result["events"], 1)
      assert event2["level"] == "ERROR"
      assert event2["userId"] == "user456"
      refute Map.has_key?(event2, "durationMs")  # Should be pruned (was nil)

      # Verify nulls were pruned from third event
      event3 = Enum.at(result["events"], 2)
      refute Map.has_key?(event3, "userId")
      refute Map.has_key?(event3, "sessionId")
      assert event3["level"] == "WARN"

      # Verify processing timestamp
      assert Map.has_key?(result, "processed_at")
      assert String.contains?(result["processed_at"], "T")
      assert result["total_events"] == 3
    end

    test "data transformation and enrichment pipeline" do
      data = %{
        "products" => [
          %{
            "id" => "prod_001",
            "name" => "Laptop Computer",
            "price_cents" => 149999,
            "category_id" => "electronics",
            "created_date" => "2024-01-15",
            "last_updated" => "2024-11-15T10:30:00Z",
            "inventory" => %{
              "in_stock" => 25,
              "reserved" => 5,
              "incoming" => 10
            },
            "supplier_info" => %{
              "supplier_id" => "sup_123",
              "contact_email" => "orders@supplier.com",
              "internal_cost_cents" => 89999
            }
          },
          %{
            "id" => "prod_002",
            "name" => "Wireless Mouse",
            "price_cents" => 2999,
            "category_id" => "electronics",
            "created_date" => "2024-03-10",
            "last_updated" => "2024-11-10T15:45:00Z",
            "inventory" => %{
              "in_stock" => 150,
              "reserved" => 12,
              "incoming" => 50
            },
            "supplier_info" => %{
              "supplier_id" => "sup_456",
              "contact_email" => "sales@mouseco.com",
              "internal_cost_cents" => 1299
            }
          }
        ]
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          # Remove sensitive supplier information
          %{"op" => "drop", "paths" => ["products[].supplier_info.internal_cost_cents", "products[].supplier_info.contact_email"]},

          # Convert price from cents to dollars
          %{"op" => "project", "path" => "products", "mapping" => %{
            "id" => "id",
            "name" => "name",
            "price_dollars" => "price_cents",  # Will need custom transformation
            "category" => "category_id",
            "created_date" => "created_date",
            "last_updated" => "last_updated",
            "total_available" => "inventory.in_stock",
            "supplier_id" => "supplier_info.supplier_id"
          }},

          # Format dates consistently
          %{"op" => "format_date", "path" => "products[].created_date", "format" => "iso8601"},
          %{"op" => "format_date", "path" => "products[].last_updated", "format" => "human"},

          # Add current timestamp
          %{"op" => "current_timestamp", "path" => "catalog_generated_at", "format" => "iso8601"},

          # Cleanup
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{
          "catalog" => "$working.products",
          "metadata" => %{
            "generated_at" => "$working.catalog_generated_at",
            "product_count" => 2,
            "version" => "1.0"
          }
        }
      }

      result = DslInterpreter.execute(data, dsl)

      # Verify catalog structure
      assert Map.has_key?(result, "catalog")
      assert Map.has_key?(result, "metadata")
      assert length(result["catalog"]) == 2

      # Check first product transformation
      product1 = Enum.at(result["catalog"], 0)
      assert product1["id"] == "prod_001"
      assert product1["name"] == "Laptop Computer"
      assert product1["category"] == "electronics"
      assert product1["total_available"] == 25
      assert product1["supplier_id"] == "sup_123"

      # Verify sensitive data was removed
      refute String.contains?(inspect(result), "internal_cost_cents")
      refute String.contains?(inspect(result), "contact_email")

      # Verify metadata
      assert result["metadata"]["product_count"] == 2
      assert result["metadata"]["version"] == "1.0"
      assert String.contains?(result["metadata"]["generated_at"], "T")
    end
  end

  describe "error handling and edge cases" do
    test "handles malformed DSL gracefully" do
      data = %{"test" => "data"}

      # Missing required fields
      malformed_dsls = [
        %{},  # Empty DSL
        %{"pipeline" => []},  # Missing output
        %{"output" => %{"result" => "$working"}},  # Missing pipeline
        %{"pipeline" => [%{"op" => "invalid_operation"}], "output" => %{"result" => "$working"}}
      ]

      for dsl <- malformed_dsls do
        result = DslInterpreter.execute(data, dsl)
        # Should not crash, might return original data or error
        assert result != nil
      end
    end

    test "handles deeply nested data structures" do
      # Create a 10-level deep nested structure
      deep_data = 1..10
      |> Enum.reduce(%{"value" => "target"}, fn i, acc ->
        %{"level#{i}" => acc}
      end)

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["level10.level9.level8.level7.level6.level5.level4.level3.level2.level1.value"]}
        ],
        "output" => %{"result" => "$working"}
      }

      result = DslInterpreter.execute(deep_data, dsl)

      # Should handle deep nesting without issues
      assert Map.has_key?(result, "result")
      # Target value should be removed
      deep_value = get_in(result, ["result", "level10", "level9", "level8", "level7", "level6", "level5", "level4", "level3", "level2", "level1", "value"])
      assert is_nil(deep_value)
    end

    test "handles large data sets efficiently" do
      # Generate a large dataset
      large_data = %{
        "items" => Enum.map(1..1000, fn i ->
          %{
            "id" => "item_#{i}",
            "name" => "Item #{i}",
            "value" => i * 10,
            "metadata" => %{
              "created" => "2024-01-#{rem(i, 28) + 1}T10:00:00Z",
              "category" => "cat_#{rem(i, 5)}"
            }
          }
        end)
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["items[].metadata"]},
          %{"op" => "rename", "mapping" => %{"items[].id" => "items[].identifier"}},
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{"processed_items" => "$working.items"}
      }

      start_time = System.monotonic_time(:millisecond)
      result = DslInterpreter.execute(large_data, dsl)
      end_time = System.monotonic_time(:millisecond)

      execution_time = end_time - start_time

      # Should process within reasonable time (< 1 second for 1000 items)
      assert execution_time < 1000

      # Verify transformations applied correctly
      assert length(result["processed_items"]) == 1000
      first_item = Enum.at(result["processed_items"], 0)
      assert Map.has_key?(first_item, "identifier")
      refute Map.has_key?(first_item, "id")
      refute Map.has_key?(first_item, "metadata")
    end
  end

  describe "performance benchmarks" do
    @tag :benchmark
    test "benchmark common operations" do
      # Create test data
      data = %{
        "users" => Enum.map(1..100, fn i ->
          %{
            "user_id" => "user_#{i}",
            "first_name" => "User",
            "last_name" => "#{i}",
            "email" => "user#{i}@example.com",
            "created_at" => "2024-01-15T10:30:00Z",
            "metadata" => %{
              "internal_id" => "internal_#{i}",
              "debug_info" => %{"trace" => "trace_#{i}"}
            }
          }
        end)
      }

      operations = [
        {"drop", %{"op" => "drop", "paths" => ["users[].metadata.debug_info"]}},
        {"rename", %{"op" => "rename", "mapping" => %{"users[].user_id" => "users[].id"}}},
        {"format_date", %{"op" => "format_date", "path" => "users[].created_at", "format" => "human"}},
        {"prune", %{"op" => "prune", "strategy" => "empty_values"}}
      ]

      for {op_name, operation} <- operations do
        times = for _ <- 1..10 do
          start_time = System.monotonic_time(:microsecond)
          DslInterpreter.apply_operation_public(data, operation)
          end_time = System.monotonic_time(:microsecond)
          end_time - start_time
        end

        avg_time = Enum.sum(times) / length(times)
        max_time = Enum.max(times)
        min_time = Enum.min(times)

        IO.puts("#{op_name} operation - Avg: #{Float.round(avg_time, 2)}μs, Min: #{min_time}μs, Max: #{max_time}μs")

        # Basic performance assertions (adjust thresholds as needed)
        assert avg_time < 50_000  # Less than 50ms on average
        assert max_time < 100_000  # Less than 100ms maximum
      end
    end
  end
end
