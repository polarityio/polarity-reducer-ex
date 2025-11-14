defmodule DslPipelineTesterTest do
  use ExUnit.Case
  doctest DslPipelineTester

  describe "test_pipeline_direct/3" do
    test "executes a simple pipeline successfully" do
      data = %{
        "users" => [
          %{"name" => "Alice", "age" => 30},
          %{"name" => "Bob", "age" => 25}
        ]
      }

      dsl_config = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["users[].age"]}
        ],
        "output" => %{"result" => "$working.users"}
      }

      result = DslPipelineTester.test_pipeline_direct(data, dsl_config, verbose: false)

      assert result.success == true
      assert length(result.steps) == 3  # root + 1 pipeline op + output
      assert result.summary.operations_executed == 1

      # Check that age was dropped
      users_result = result.final_result["result"]
      assert is_list(users_result)
      assert length(users_result) == 2

      Enum.each(users_result, fn user ->
        assert Map.has_key?(user, "name")
        refute Map.has_key?(user, "age")
      end)
    end

    test "handles complex pipeline with multiple operations" do
      data = %{
        "events" => [
          %{
            "id" => 1,
            "config" => [
              %{"key" => "theme", "value" => "dark"},
              %{"key" => "lang", "value" => "en"}
            ]
          }
        ]
      }

      dsl_config = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{
            "op" => "list_to_map",
            "path" => "events[].config",
            "key_from" => "key",
            "value_from" => "value"
          },
          %{"op" => "drop", "paths" => ["events[].id"]}
        ],
        "output" => %{"events" => "$working.events"}
      }

      result = DslPipelineTester.test_pipeline_direct(data, dsl_config, verbose: false)

      assert result.success == true
      assert length(result.steps) == 4  # root + 2 pipeline ops + output
      assert result.summary.operations_executed == 2

      # Check transformations
      events = result.final_result["events"]
      assert length(events) == 1

      event = hd(events)
      refute Map.has_key?(event, "id")
      assert event["config"]["theme"] == "dark"
      assert event["config"]["lang"] == "en"
    end

    test "provides detailed step information" do
      data = %{"test" => "data"}
      dsl_config = %{
        "root" => %{"path" => ""},
        "pipeline" => [%{"op" => "prune", "strategy" => "empty_values"}],
        "output" => %{"result" => "$working"}
      }

      result = DslPipelineTester.test_pipeline_direct(data, dsl_config, verbose: false)

      assert result.success == true

      # Check step details
      steps = result.steps
      assert length(steps) == 3

      # Root step
      root_step = Enum.at(steps, 0)
      assert root_step.step == 0
      assert root_step.operation == "root_resolution"
      assert root_step.input == data
      assert root_step.output == data

      # Pipeline step
      pipeline_step = Enum.at(steps, 1)
      assert pipeline_step.step == 1
      assert pipeline_step.operation == "prune"
      assert String.contains?(pipeline_step.description, "empty values")
      assert Map.has_key?(pipeline_step, :execution_time_ms)

      # Output step
      output_step = Enum.at(steps, 2)
      assert output_step.step == 2
      assert output_step.operation == "output_building"
      assert output_step.output == %{"result" => %{"test" => "data"}}
    end

    test "handles errors gracefully" do
      data = %{"test" => "data"}
      # Completely invalid DSL config
      invalid_dsl = "not a map"

      result = DslPipelineTester.test_pipeline_direct(data, invalid_dsl, verbose: false)

      assert result.success == false
      assert Map.has_key?(result, :error)
    end
  end

  describe "analyze_pipeline/1" do
    test "analyzes pipeline complexity correctly" do
      dsl_config = %{
        "root" => %{"path" => "data"},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["field1"]},
          %{"op" => "project", "path" => "items", "mapping" => %{"new" => "old"}},
          %{"op" => "list_to_map", "path" => "list", "key_from" => "k", "value_from" => "v"}
        ],
        "output" => %{"result" => "$working"}
      }

      analysis = DslPipelineTester.analyze_pipeline(dsl_config)

      assert analysis.total_operations == 3
      assert analysis.complexity_score > 3  # Base + weighted scores
      assert analysis.operation_types["drop"] == 1
      assert analysis.operation_types["project"] == 1
      assert analysis.operation_types["list_to_map"] == 1
    end

    test "detects potential issues" do
      # Pipeline with consecutive prune operations
      dsl_config = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "prune", "strategy" => "empty_values"},
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{"result" => "$working"}
      }

      analysis = DslPipelineTester.analyze_pipeline(dsl_config)

      assert length(analysis.potential_issues) > 0
      assert Enum.any?(analysis.potential_issues, fn issue ->
        String.contains?(issue, "consecutive prune")
      end)
    end

    test "provides recommendations" do
      dsl_config = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["field"]},
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{"result" => "$working"}
      }

      analysis = DslPipelineTester.analyze_pipeline(dsl_config)

      assert length(analysis.recommendations) > 0
      assert Enum.any?(analysis.recommendations, fn rec ->
        String.contains?(rec, "prune operations")
      end)
    end
  end

  describe "load_pipeline_config/1" do
    setup do
      # Create a temporary valid config file
      config = %{
        "root" => %{"path" => "test"},
        "pipeline" => [%{"op" => "drop", "paths" => ["field"]}],
        "output" => %{"result" => "$working"}
      }

      config_path = "/tmp/test_pipeline.json"
      File.write!(config_path, Jason.encode!(config))

      on_exit(fn -> File.rm(config_path) end)

      %{config_path: config_path, config: config}
    end

    test "loads valid configuration successfully", %{config_path: config_path, config: expected_config} do
      case DslPipelineTester.load_pipeline_config(config_path) do
        {:ok, loaded_config} ->
          assert loaded_config == expected_config
        {:error, reason} ->
          flunk("Should have loaded successfully, got error: #{reason}")
      end
    end

    test "handles missing file" do
      case DslPipelineTester.load_pipeline_config("/nonexistent/file.json") do
        {:error, reason} ->
          assert String.contains?(reason, "File read error")
        {:ok, _} ->
          flunk("Should have failed with file error")
      end
    end

    test "handles invalid JSON" do
      invalid_json_path = "/tmp/invalid.json"
      File.write!(invalid_json_path, "{invalid json")
      on_exit(fn -> File.rm(invalid_json_path) end)

      case DslPipelineTester.load_pipeline_config(invalid_json_path) do
        {:error, reason} ->
          assert String.contains?(reason, "JSON decode error")
        {:ok, _} ->
          flunk("Should have failed with JSON error")
      end
    end
  end

  describe "performance tracking" do
    test "tracks execution time for operations" do
      data = %{"items" => Enum.to_list(1..100)}  # Larger dataset

      dsl_config = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "truncate_list", "path" => "items", "max_size" => 50, "shape" => %{"count" => "$length"}},
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{"result" => "$working"}
      }

      result = DslPipelineTester.test_pipeline_direct(data, dsl_config, verbose: false)

      assert result.success == true
      assert result.total_execution_time_ms >= 0

      # Check that individual steps have timing info
      pipeline_steps = result.steps |> Enum.filter(&(&1.step > 0 and &1.step < length(result.steps)))
      Enum.each(pipeline_steps, fn step ->
        assert Map.has_key?(step, :execution_time_ms)
        assert is_integer(step.execution_time_ms)
      end)
    end

    test "calculates data size reduction" do
      # Create data that should be reduced significantly
      large_data = %{
        "events" => Enum.map(1..10, fn i ->
          %{
            "id" => i,
            "data" => String.duplicate("x", 100),  # Large strings
            "metadata" => %{"debug" => "remove_me"}
          }
        end)
      }

      dsl_config = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["events[].data", "events[].metadata"]}
        ],
        "output" => %{"result" => "$working.events"}
      }

      result = DslPipelineTester.test_pipeline_direct(large_data, dsl_config, verbose: false)

      assert result.success == true
      # Should show significant size reduction
      size_change = result.summary.data_size_reduction
      assert String.contains?(size_change, "-") or String.contains?(size_change, "+")
    end
  end
end
