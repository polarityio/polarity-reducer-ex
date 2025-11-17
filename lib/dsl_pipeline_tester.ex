defmodule PolarityReducerEx.DslPipelineTester do
  @moduledoc """
  SDK for testing DSL pipelines with step-by-step execution and visualization.

  This module helps developers understand how their DSL transformations work by:
  - Loading example data and pipeline configurations from JSON files
  - Executing pipeline operations step-by-step
  - Showing intermediate results after each transformation
  - Providing detailed analysis and debugging information
  """

  alias PolarityReducerEx.DslInterpreter
  require Logger

  @doc """
  Test a pipeline with step-by-step execution and detailed output.

  ## Parameters
  - `data_file_path`: Path to JSON file containing example data
  - `pipeline_file_path`: Path to JSON file containing DSL configuration

  ## Returns
  A detailed test report showing each step of the transformation

  ## Example
      DslPipelineTester.test_pipeline("examples/data.json", "examples/pipeline.json")
  """
  def test_pipeline(data_file_path, pipeline_file_path) do
    with {:ok, data} <- load_json_file(data_file_path),
         {:ok, dsl_config} <- load_json_file(pipeline_file_path) do

      IO.puts("🧪 DSL Pipeline Test Report")
      IO.puts("=" |> String.duplicate(50))

      test_result = execute_pipeline_with_steps(data, dsl_config)
      display_test_report(test_result)

      test_result
    else
      {:error, reason} ->
        IO.puts("❌ Error: #{reason}")
        {:error, reason}
    end
  end

  @doc """
  Test a pipeline directly with provided data and DSL configuration.

  ## Parameters
  - `data`: The input data map
  - `dsl_config`: The DSL configuration map
  - `opts`: Options for the test (default: [])

  ## Options
  - `:verbose`: Show detailed information for each step (default: true)
  - `:show_diffs`: Show before/after differences (default: true)

  ## Returns
  A detailed test result with step-by-step execution data
  """
  def test_pipeline_direct(data, dsl_config, opts \\ []) do
    verbose = Keyword.get(opts, :verbose, true)

    if verbose do
      IO.puts("🧪 DSL Pipeline Test Report")
      IO.puts("=" |> String.duplicate(50))
    end

    test_result = execute_pipeline_with_steps(data, dsl_config)

    if verbose do
      display_test_report(test_result)
    end

    test_result
  end

  @doc """
  Load and validate a DSL configuration from a JSON file.

  ## Parameters
  - `file_path`: Path to the JSON file containing DSL configuration

  ## Returns
  - `{:ok, dsl_config}` on success
  - `{:error, reason}` on failure
  """
  def load_pipeline_config(file_path) do
    case load_json_file(file_path) do
      {:ok, config} ->
        case validate_dsl_config(config) do
          :ok -> {:ok, config}
          {:error, reason} -> {:error, "Invalid DSL config: #{reason}"}
        end
      error -> error
    end
  end

  @doc """
  Analyze a pipeline configuration and provide insights.

  ## Parameters
  - `dsl_config`: The DSL configuration to analyze

  ## Returns
  Analysis report with insights about the pipeline
  """
  def analyze_pipeline(dsl_config) when is_map(dsl_config) do
    pipeline = dsl_config["pipeline"] || []

    analysis = %{
      total_operations: length(pipeline),
      operation_types: count_operation_types(pipeline),
      complexity_score: calculate_complexity_score(pipeline),
      potential_issues: detect_potential_issues(dsl_config),
      recommendations: generate_recommendations(dsl_config)
    }

    display_pipeline_analysis(analysis)
    analysis
  end

  # ===== PRIVATE FUNCTIONS =====

  # Execute the pipeline step by step, capturing intermediate results
  defp execute_pipeline_with_steps(data, dsl_config) do
    start_time = System.monotonic_time(:millisecond)

    # Step 1: Resolve root object
    {working_map, original_data} = resolve_root_object(data, dsl_config["root"])

    root_step = %{
      step: 0,
      operation: "root_resolution",
      description: describe_root_operation(dsl_config["root"]),
      input: data,
      output: working_map,
      execution_time_ms: 0
    }

    # Step 2: Execute pipeline operations one by one
    pipeline = dsl_config["pipeline"] || []
    {final_working, pipeline_steps} = execute_pipeline_steps(working_map, pipeline)

    # Step 3: Build output object
    final_output = build_output_object(original_data, final_working, dsl_config["output"] || %{})

    output_step = %{
      step: length(pipeline) + 1,
      operation: "output_building",
      description: describe_output_operation(dsl_config["output"]),
      input: final_working,
      output: final_output,
      execution_time_ms: 0
    }

    end_time = System.monotonic_time(:millisecond)
    total_time = end_time - start_time

    %{
      success: true,
      original_data: original_data,
      final_result: final_output,
      total_execution_time_ms: total_time,
      steps: [root_step] ++ pipeline_steps ++ [output_step],
      summary: %{
        total_steps: length(pipeline) + 2,
        operations_executed: length(pipeline),
        data_size_reduction: calculate_size_reduction(data, final_output)
      }
    }
  rescue
    error ->
      %{
        success: false,
        error: Exception.message(error),
        error_type: error.__struct__,
        stacktrace: __STACKTRACE__
      }
  end

  # Execute each pipeline operation individually
  defp execute_pipeline_steps(initial_working, pipeline) do
    {final_working, steps} =
      pipeline
      |> Enum.with_index(1)
      |> Enum.reduce({initial_working, []}, fn {operation, index}, {current_working, acc_steps} ->
        step_start = System.monotonic_time(:millisecond)

        result_working = apply_single_operation(current_working, operation)

        step_end = System.monotonic_time(:millisecond)
        execution_time = step_end - step_start

        step_info = %{
          step: index,
          operation: operation["op"] || "unknown",
          description: describe_operation(operation),
          input: current_working,
          output: result_working,
          execution_time_ms: execution_time,
          operation_config: operation
        }

        {result_working, acc_steps ++ [step_info]}
      end)

    {final_working, steps}
  end

  # Apply a single operation using unified DslInterpreter function
  defp apply_single_operation(working_map, operation) do
    DslInterpreter.apply_operation_public(working_map, operation)
  end

  # Delegate to DslInterpreter public helper functions
  defp resolve_root_object(data_map, root_config) do
    DslInterpreter.resolve_root_object_public(data_map, root_config)
  end

  defp build_output_object(original_data, working_data, output_spec) do
    DslInterpreter.build_output_object_public(original_data, working_data, output_spec)
  end

  # ===== UTILITY FUNCTIONS =====

  # Load JSON file
  defp load_json_file(file_path) do
    case File.read(file_path) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, data} -> {:ok, data}
          {:error, %Jason.DecodeError{} = error} ->
            {:error, "JSON decode error: #{Exception.message(error)}"}
        end
      {:error, reason} ->
        {:error, "File read error: #{:file.format_error(reason)}"}
    end
  end

  # Validate DSL configuration
  defp validate_dsl_config(config) when is_map(config) do
    # Only pipeline is truly required, root and output have defaults
    required_keys = ["pipeline"]
    missing_keys = Enum.filter(required_keys, &(not Map.has_key?(config, &1)))

    case missing_keys do
      [] -> :ok
      keys -> {:error, "Missing required keys: #{Enum.join(keys, ", ")}"}
    end
  end

  defp validate_dsl_config(_), do: {:error, "DSL config must be a map"}

  # ===== DESCRIPTION FUNCTIONS =====

  defp describe_root_operation(nil), do: "Using entire input as working object"
  defp describe_root_operation(root_config) when is_map(root_config) do
    path = root_config["path"] || ""
    on_null = root_config["on_null"] || "return_empty"

    if path == "" do
      "Using entire input as working object"
    else
      "Extracting '#{path}' as working object (on_null: #{on_null})"
    end
  end

  defp describe_output_operation(nil), do: "Returning working object as final result"
  defp describe_output_operation(output_config) when is_map(output_config) do
    keys = Map.keys(output_config)
    case length(keys) do
      0 -> "Returning working object as final result"
      1 -> "Building output with 1 field: #{hd(keys)}"
      n -> "Building output with #{n} fields: #{Enum.join(keys, ", ")}"
    end
  end

  defp describe_operation(%{"op" => "drop", "paths" => paths}) do
    case length(paths) do
      1 -> "Dropping path: #{hd(paths)}"
      n -> "Dropping #{n} paths: #{Enum.join(paths, ", ")}"
    end
  end

  defp describe_operation(%{"op" => "project", "path" => path, "mapping" => mapping}) do
    fields = Map.keys(mapping)
    "Projecting #{length(fields)} fields at '#{path}': #{Enum.join(fields, ", ")}"
  end

  defp describe_operation(%{"op" => "project_and_replace", "projection" => projection}) do
    fields = Map.keys(projection)
    "Replacing working object with #{length(fields)} projected fields: #{Enum.join(fields, ", ")}"
  end

  defp describe_operation(%{"op" => "list_to_map", "path" => path, "key_from" => key_from, "value_from" => value_from}) do
    "Converting list at '#{path}' to map using key='#{key_from}' and value='#{value_from}'"
  end

  defp describe_operation(%{"op" => "truncate_list", "path" => path, "max_size" => max_size}) do
    "Truncating list at '#{path}' to maximum #{max_size} items"
  end

  defp describe_operation(%{"op" => "prune"}) do
    "Removing empty values (nil, \"\", {}, []) recursively"
  end

  defp describe_operation(%{"op" => "rename", "mapping" => mapping}) do
    renames = Enum.map(mapping, fn {from, to} -> "#{from} → #{to}" end)
    "Renaming fields: #{Enum.join(renames, ", ")}"
  end

  defp describe_operation(%{"op" => "format_date", "path" => path, "format" => format}) do
    "Formatting date at '#{path}' to #{format} format"
  end

  defp describe_operation(%{"op" => "parse_date", "path" => path}) do
    "Parsing date string at '#{path}' to standardized format"
  end

  defp describe_operation(%{"op" => "date_add", "path" => path, "amount" => amount, "unit" => unit}) do
    "Adding #{amount} #{unit} to date at '#{path}'"
  end

  defp describe_operation(%{"op" => "date_diff", "from_path" => from_path, "to_path" => to_path, "result_path" => result_path}) do
    "Calculating date difference from '#{from_path}' to '#{to_path}', storing result in '#{result_path}'"
  end

  defp describe_operation(%{"op" => "current_timestamp", "path" => path}) do
    "Setting current timestamp at '#{path}'"
  end

  defp describe_operation(%{"op" => op}) do
    "Executing '#{op}' operation"
  end

  defp describe_operation(_), do: "Unknown operation"

  # ===== ANALYSIS FUNCTIONS =====

  defp count_operation_types(pipeline) do
    pipeline
    |> Enum.map(&(&1["op"]))
    |> Enum.frequencies()
  end

  defp calculate_complexity_score(pipeline) do
    base_score = length(pipeline)

    complexity_weights = %{
      "drop" => 1,
      "project" => 2,
      "project_and_replace" => 3,
      "list_to_map" => 3,
      "truncate_list" => 2,
      "aggregate_list" => 4,
      "prune" => 2
    }

    weighted_score =
      pipeline
      |> Enum.map(&(Map.get(complexity_weights, &1["op"], 2)))
      |> Enum.sum()

    base_score + weighted_score
  end

  defp detect_potential_issues(dsl_config) do
    issues = []
    pipeline = dsl_config["pipeline"] || []

    issues = if length(pipeline) > 10 do
      ["Pipeline has many operations (#{length(pipeline)}) - consider splitting" | issues]
    else
      issues
    end

    issues = if has_consecutive_prune_operations?(pipeline) do
      ["Multiple consecutive prune operations detected - can be optimized" | issues]
    else
      issues
    end

    issues
  end

  defp generate_recommendations(dsl_config) do
    recommendations = []
    pipeline = dsl_config["pipeline"] || []

    recommendations = if Enum.any?(pipeline, &(&1["op"] == "prune")) do
      ["Consider placing prune operations at the end of the pipeline for better performance" | recommendations]
    else
      recommendations
    end

    recommendations
  end

  defp has_consecutive_prune_operations?(pipeline) do
    pipeline
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.any?(fn [op1, op2] -> op1["op"] == "prune" and op2["op"] == "prune" end)
  end

  # ===== DISPLAY FUNCTIONS =====

  defp display_test_report(%{success: false, error: error}) do
    IO.puts("❌ Pipeline execution failed!")
    IO.puts("Error: #{error}")
  end

  defp display_test_report(%{success: true} = result) do
    IO.puts("✅ Pipeline executed successfully!")
    IO.puts("")

    display_summary(result.summary)
    IO.puts("")

    display_step_details(result.steps)
    IO.puts("")

    display_performance_metrics(result)
  end

  defp display_summary(summary) do
    IO.puts("📊 Execution Summary:")
    IO.puts("  • Total steps: #{summary.total_steps}")
    IO.puts("  • Operations executed: #{summary.operations_executed}")
    IO.puts("  • Data size change: #{summary.data_size_reduction}%")
  end

  defp display_step_details(steps) do
    IO.puts("🔍 Step-by-Step Execution:")

    Enum.each(steps, fn step ->
      IO.puts("")
      IO.puts("  Step #{step.step}: #{step.operation}")
      IO.puts("  Description: #{step.description}")

      if step.step > 0 and step.step < length(steps) do
        IO.puts("  Input size: #{map_size_info(step.input)}")
        IO.puts("  Output size: #{map_size_info(step.output)}")

        if step.execution_time_ms > 0 do
          IO.puts("  Execution time: #{step.execution_time_ms}ms")
        end

        # Show a preview of the transformation
        show_transformation_preview(step.input, step.output)
      end
    end)
  end

  defp display_performance_metrics(result) do
    IO.puts("⚡ Performance Metrics:")
    IO.puts("  • Total execution time: #{result.total_execution_time_ms}ms")

    slowest_step = result.steps
                   |> Enum.filter(&(&1.execution_time_ms > 0))
                   |> Enum.max_by(&(&1.execution_time_ms), fn -> nil end)

    if slowest_step do
      IO.puts("  • Slowest operation: #{slowest_step.operation} (#{slowest_step.execution_time_ms}ms)")
    end
  end

  defp display_pipeline_analysis(analysis) do
    IO.puts("🔍 Pipeline Analysis Report")
    IO.puts("=" |> String.duplicate(40))
    IO.puts("Total operations: #{analysis.total_operations}")
    IO.puts("Complexity score: #{analysis.complexity_score}")
    IO.puts("")

    IO.puts("Operation types:")
    Enum.each(analysis.operation_types, fn {op, count} ->
      IO.puts("  • #{op}: #{count}")
    end)

    if length(analysis.potential_issues) > 0 do
      IO.puts("")
      IO.puts("⚠️  Potential issues:")
      Enum.each(analysis.potential_issues, fn issue ->
        IO.puts("  • #{issue}")
      end)
    end

    if length(analysis.recommendations) > 0 do
      IO.puts("")
      IO.puts("💡 Recommendations:")
      Enum.each(analysis.recommendations, fn rec ->
        IO.puts("  • #{rec}")
      end)
    end
  end

  # ===== HELPER FUNCTIONS =====

  defp map_size_info(data) when is_map(data) do
    "#{map_size(data)} keys"
  end

  defp map_size_info(data) when is_list(data) do
    "#{length(data)} items"
  end

  defp map_size_info(_), do: "primitive value"

  defp show_transformation_preview(input, output) do
    # Show a simplified diff for small data structures
    if is_small_enough_to_display?(input) and is_small_enough_to_display?(output) do
      IO.puts("  Before: #{inspect(input, limit: :infinity, pretty: true, width: 60)}")
      IO.puts("  After:  #{inspect(output, limit: :infinity, pretty: true, width: 60)}")
    else
      IO.puts("  [Data too large to display - showing structure only]")
      IO.puts("  Before: #{data_structure_summary(input)}")
      IO.puts("  After:  #{data_structure_summary(output)}")
    end
  end

  defp is_small_enough_to_display?(data) do
    serialized = inspect(data)
    String.length(serialized) < 200
  end

  defp data_structure_summary(data) when is_map(data) do
    keys = Map.keys(data) |> Enum.take(3)
    remaining = max(0, map_size(data) - 3)
    key_preview = if remaining > 0 do
      Enum.join(keys, ", ") <> " (+#{remaining} more)"
    else
      Enum.join(keys, ", ")
    end
    "Map with #{map_size(data)} keys: [#{key_preview}]"
  end

  defp data_structure_summary(data) when is_list(data) do
    "List with #{length(data)} items"
  end

  defp data_structure_summary(data) do
    "#{typeof(data)}: #{inspect(data, limit: 20)}"
  end

  defp typeof(data) when is_binary(data), do: "String"
  defp typeof(data) when is_integer(data), do: "Integer"
  defp typeof(data) when is_float(data), do: "Float"
  defp typeof(data) when is_boolean(data), do: "Boolean"
  defp typeof(nil), do: "Nil"
  defp typeof(_), do: "Unknown"

  defp calculate_size_reduction(original, final) do
    original_size = :erlang.external_size(original)
    final_size = :erlang.external_size(final)

    if original_size > 0 do
      reduction = ((original_size - final_size) / original_size * 100) |> Float.round(1)
      if reduction >= 0, do: "+#{reduction}", else: "#{reduction}"
    else
      "0"
    end
  end
end
