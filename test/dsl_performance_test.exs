defmodule PolarityReducerEx.DslPerformanceTest do
  use ExUnit.Case

  alias PolarityReducerEx.DslInterpreter

  @moduledoc """
  Performance and stress tests for the DSL interpreter.
  These tests verify that the system performs well under various loads.
  """

  describe "performance tests" do
    @describetag :performance
    test "processes large datasets efficiently" do
      # Generate 5000 records
      large_dataset = %{
        "records" => Enum.map(1..5000, fn i ->
          %{
            "id" => i,
            "name" => "Record #{i}",
            "email" => "user#{i}@example.com",
            "created_at" => "2024-#{rem(i, 12) + 1 |> Integer.to_string() |> String.pad_leading(2, "0")}-#{rem(i, 28) + 1 |> Integer.to_string() |> String.pad_leading(2, "0")}T10:00:00Z",
            "metadata" => %{
              "source" => "import_#{rem(i, 10)}",
              "processed" => false,
              "tags" => ["tag_#{rem(i, 5)}", "category_#{rem(i, 3)}"],
              "internal" => %{
                "trace_id" => "trace_#{i}",
                "debug_level" => rem(i, 4)
              }
            },
            "preferences" => %{
              "notifications" => rem(i, 2) == 0,
              "theme" => if(rem(i, 2) == 0, do: "dark", else: "light"),
              "language" => "en"
            }
          }
        end)
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          # Remove internal debugging information
          %{"op" => "drop", "paths" => ["records[].metadata.internal"]},

          # Rename fields for API consistency
          %{"op" => "rename", "mapping" => %{
            "records[].created_at" => "records[].createdAt",
            "records[].metadata.source" => "records[].metadata.importSource"
          }},

          # Format all creation dates
          %{"op" => "format_date", "path" => "records[].createdAt", "format" => "human"},

          # Clean up any empty values
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{
          "data" => "$working.records",
          "count" => 5000
        }
      }

      # Measure execution time
      start_time = System.monotonic_time(:millisecond)
      result = DslInterpreter.execute(large_dataset, dsl)
      end_time = System.monotonic_time(:millisecond)

      execution_time = end_time - start_time

      # Performance assertions
      assert execution_time < 2000, "Large dataset processing took #{execution_time}ms, expected < 2000ms"

      # Verify correctness
      assert length(result["data"]) == 5000
      assert result["count"] == 5000

      # Sample verification
      sample_record = Enum.at(result["data"], 100)
      assert Map.has_key?(sample_record, "createdAt")
      refute Map.has_key?(sample_record, "created_at")
      refute get_in(sample_record, ["metadata", "internal"])
      assert String.contains?(sample_record["createdAt"], " ") # Human format

      IO.puts("Large dataset (5000 records) processed in #{execution_time}ms")
    end

    test "handles deeply nested structures efficiently" do
      # Create deeply nested structure (10 levels to avoid infinite loops)
      deep_data = 1..10
      |> Enum.reduce(%{"target" => "deep_value", "other" => "keep_me"}, fn level, acc ->
        %{
          "level_#{level}" => acc,
          "data_#{level}" => "value_#{level}"
        }
      end)

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          # Drop deeply nested target
          %{"op" => "drop", "paths" => [
            "level_10.level_9.level_8.level_7.level_6.level_5.level_4.level_3.level_2.level_1.target"
          ]},

          # Rename some deeply nested fields
          %{"op" => "rename", "mapping" => %{
            "level_10.level_9.level_8.level_7.level_6.level_5.level_4.level_3.level_2.level_1.other" => "level_10.level_9.level_8.level_7.level_6.level_5.level_4.level_3.level_2.level_1.renamed"
          }}
        ],
        "output" => %{"result" => "$working"}
      }

      start_time = System.monotonic_time(:millisecond)
      result = DslInterpreter.execute(deep_data, dsl)
      end_time = System.monotonic_time(:millisecond)

      execution_time = end_time - start_time

      # Should handle deep nesting without significant performance impact
      assert execution_time < 100, "Deep nesting processing took #{execution_time}ms, expected < 100ms"

      # Verify transformation worked
      deep_path = ["result", "level_10", "level_9", "level_8", "level_7", "level_6", "level_5", "level_4", "level_3", "level_2", "level_1"]
      deep_object = get_in(result, deep_path)

      refute Map.has_key?(deep_object, "target")
      assert Map.has_key?(deep_object, "renamed")
      assert deep_object["renamed"] == "keep_me"

      IO.puts("Deep nesting (10 levels) processed in #{execution_time}ms")
    end

    test "array processing performance" do
      # Create data with large arrays at multiple levels
      array_data = %{
        "departments" => Enum.map(1..50, fn dept_id ->
          %{
            "id" => dept_id,
            "name" => "Department #{dept_id}",
            "employees" => Enum.map(1..100, fn emp_id ->
              %{
                "employee_id" => "#{dept_id}_#{emp_id}",
                "first_name" => "Employee",
                "last_name" => "#{emp_id}",
                "email" => "emp#{dept_id}_#{emp_id}@company.com",
                "hire_date" => "2024-01-15T10:00:00Z",
                "projects" => Enum.map(1..5, fn proj_id ->
                  %{
                    "project_id" => "proj_#{dept_id}_#{emp_id}_#{proj_id}",
                    "name" => "Project #{proj_id}",
                    "start_date" => "2024-02-01T09:00:00Z",
                    "status" => if(rem(proj_id, 2) == 0, do: "active", else: "completed")
                  }
                end)
              }
            end)
          }
        end)
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          # Rename fields across all nested arrays
          %{"op" => "rename", "mapping" => %{
            "departments[].employees[].employee_id" => "departments[].employees[].id",
            "departments[].employees[].first_name" => "departments[].employees[].firstName",
            "departments[].employees[].last_name" => "departments[].employees[].lastName",
            "departments[].employees[].hire_date" => "departments[].employees[].hireDate",
            "departments[].employees[].projects[].project_id" => "departments[].employees[].projects[].id",
            "departments[].employees[].projects[].start_date" => "departments[].employees[].projects[].startDate"
          }},

          # Format dates in nested arrays
          %{"op" => "format_date", "path" => "departments[].employees[].hireDate", "format" => "date_only"},
          %{"op" => "format_date", "path" => "departments[].employees[].projects[].startDate", "format" => "date_only"},

          # Clean up
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{"result" => "$working"}
      }

      start_time = System.monotonic_time(:millisecond)
      result = DslInterpreter.execute(array_data, dsl)
      end_time = System.monotonic_time(:millisecond)

      execution_time = end_time - start_time

      # This is processing 50 * 100 * 5 = 25,000 nested objects
      assert execution_time < 5000, "Large nested array processing took #{execution_time}ms, expected < 5000ms"

      # Verify structure and transformations
      assert length(result["result"]["departments"]) == 50
      assert length(result["result"]["departments"] |> Enum.at(0) |> Map.get("employees")) == 100
      assert length(result["result"]["departments"] |> Enum.at(0) |> Map.get("employees") |> Enum.at(0) |> Map.get("projects")) == 5

      # Sample verification
      sample_employee = get_in(result, ["result", "departments", Access.at(0), "employees", Access.at(0)])
      assert Map.has_key?(sample_employee, "firstName")
      assert Map.has_key?(sample_employee, "lastName")
      assert Map.has_key?(sample_employee, "hireDate")
      refute Map.has_key?(sample_employee, "first_name")
      refute Map.has_key?(sample_employee, "hire_date")

      sample_project = get_in(result, ["result", "departments", Access.at(0), "employees", Access.at(0), "projects", Access.at(0)])
      assert Map.has_key?(sample_project, "startDate")
      assert sample_project["startDate"] == "2024-02-01"  # Date only format

      IO.puts("Large nested arrays (25K objects) processed in #{execution_time}ms")
    end

    test "date operation performance" do
      # Create dataset with many date fields
      date_data = %{
        "events" => Enum.map(1..2000, fn i ->
          %{
            "id" => i,
            "created_at" => "#{2024 - rem(i, 5)}-#{rem(i, 12) + 1 |> Integer.to_string() |> String.pad_leading(2, "0")}-#{rem(i, 28) + 1 |> Integer.to_string() |> String.pad_leading(2, "0")}T#{rem(i, 24) |> Integer.to_string() |> String.pad_leading(2, "0")}:#{rem(i, 60) |> Integer.to_string() |> String.pad_leading(2, "0")}:00Z",
            "updated_at" => "#{2024}-#{rem(i, 12) + 1 |> Integer.to_string() |> String.pad_leading(2, "0")}-#{rem(i, 28) + 1 |> Integer.to_string() |> String.pad_leading(2, "0")}T#{rem(i + 1, 24) |> Integer.to_string() |> String.pad_leading(2, "0")}:#{rem(i + 1, 60) |> Integer.to_string() |> String.pad_leading(2, "0")}:00Z",
            "scheduled_for" => "#{2025}-01-#{rem(i, 28) + 1 |> Integer.to_string() |> String.pad_leading(2, "0")}T10:00:00Z"
          }
        end)
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          # Format all date fields
          %{"op" => "format_date", "path" => "events[].created_at", "format" => "human"},
          %{"op" => "format_date", "path" => "events[].updated_at", "format" => "date_only"},
          %{"op" => "format_date", "path" => "events[].scheduled_for", "format" => "iso8601"},

          # Add current timestamp
          %{"op" => "current_timestamp", "path" => "processed_at", "format" => "iso8601"}
        ],
        "output" => %{"result" => "$working"}
      }

      start_time = System.monotonic_time(:millisecond)
      result = DslInterpreter.execute(date_data, dsl)
      end_time = System.monotonic_time(:millisecond)

      execution_time = end_time - start_time

      # Processing 6000 date operations (3 per record * 2000 records)
      assert execution_time < 3000, "Date operations took #{execution_time}ms, expected < 3000ms"

      # Verify date formatting
      sample_event = Enum.at(result["result"]["events"], 0)
      assert String.contains?(sample_event["created_at"], " ")      # Human format
      assert Regex.match?(~r/^\d{4}-\d{2}-\d{2}$/, sample_event["updated_at"])  # Date only
      assert String.contains?(sample_event["scheduled_for"], "T")   # ISO format

      assert Map.has_key?(result["result"], "processed_at")

      IO.puts("Date operations (6K operations) processed in #{execution_time}ms")
    end
  end

  describe "stress tests" do
    @describetag :stress
    test "memory usage with large datasets" do
      # Monitor memory usage during large dataset processing
      initial_memory = :erlang.memory(:total)

      # Generate very large dataset
      huge_dataset = %{
        "records" => Enum.map(1..10_000, fn i ->
          %{
            "id" => i,
            "data" => String.duplicate("x", 100),  # 100 character string per record
            "nested" => %{
              "field1" => "value_#{i}",
              "field2" => i * 2,
              "field3" => %{
                "deep" => "data_#{i}",
                "numbers" => Enum.to_list(1..10)
              }
            }
          }
        end)
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["records[].nested.field3.numbers"]},
          %{"op" => "rename", "mapping" => %{"records[].data" => "records[].content"}},
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{"result" => "$working.records"}
      }

      result = DslInterpreter.execute(huge_dataset, dsl)

      final_memory = :erlang.memory(:total)
      memory_used = final_memory - initial_memory

      # Verify processing completed successfully
      assert length(result["result"]) == 10_000

      # Memory usage should be reasonable (adjust threshold as needed)
      memory_mb = memory_used / (1024 * 1024)
      IO.puts("Memory used for 10K records: #{Float.round(memory_mb, 2)} MB")

      # Should not use excessive memory (threshold may need adjustment)
      assert memory_mb < 500, "Memory usage #{memory_mb} MB exceeded threshold"
    end

    test "handles concurrent processing" do
      # Test concurrent execution of multiple DSL operations
      data = %{
        "items" => Enum.map(1..100, fn i ->
          %{"id" => i, "name" => "Item #{i}", "value" => i * 10}
        end)
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "rename", "mapping" => %{"items[].id" => "items[].identifier"}},
          %{"op" => "drop", "paths" => ["items[].value"]},
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{"result" => "$working.items"}
      }

      # Run multiple concurrent executions
      tasks = Enum.map(1..10, fn _i ->
        Task.async(fn ->
          start_time = System.monotonic_time(:millisecond)
          result = DslInterpreter.execute(data, dsl)
          end_time = System.monotonic_time(:millisecond)
          {result, end_time - start_time}
        end)
      end)

      results = Task.await_many(tasks, 5000)

      # All executions should complete successfully
      assert length(results) == 10

      execution_times = Enum.map(results, fn {_result, time} -> time end)
      max_time = Enum.max(execution_times)
      avg_time = Enum.sum(execution_times) / length(execution_times)

      # Verify all results are consistent
      first_result = results |> Enum.at(0) |> elem(0)
      for {result, _time} <- results do
        assert result == first_result
      end

      IO.puts("Concurrent execution - Avg: #{Float.round(avg_time, 2)}ms, Max: #{max_time}ms")

      # Performance should not degrade significantly under concurrent load
      assert max_time < 1000
      assert avg_time < 500
    end
  end
end
