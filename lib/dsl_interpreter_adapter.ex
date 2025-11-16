defmodule PolarityReducerEx.DslInterpreterAdapter do
  @moduledoc """
  Adapter module that wraps the original DslInterpreter for compatibility with external testing SDKs.

  This adapter provides a simple interface for the original DslInterpreter module.
  It can be used with external testing frameworks that expect specific function signatures.

  ## Usage

      # Use directly for basic execution
      result = DslInterpreterAdapter.execute(data, dsl_config)

      # Use for step-by-step execution (simulated)
      result = DslInterpreterAdapter.execute_with_steps(data, dsl_config)
  """

  alias PolarityReducerEx.DslInterpreter

  def execute(data, dsl_config) do
    # Delegate to the original DslInterpreter
    DslInterpreter.execute(data, dsl_config)
  end

  def execute_with_steps(data, dsl_config) do
    start_time = System.monotonic_time(:millisecond)

    try do
      # Execute using the main DslInterpreter
      final_result = DslInterpreter.execute(data, dsl_config)

      end_time = System.monotonic_time(:millisecond)
      total_time = end_time - start_time

      # Create simulated steps based on the pipeline operations
      pipeline = dsl_config["pipeline"] || []
      steps = Enum.with_index(pipeline, 1)
      |> Enum.map(fn {operation, index} ->
        %{
          step: index,
          operation: operation["op"] || "unknown",
          description: create_operation_description(operation),
          input: data,  # Simplified - using original data for all steps
          output: (if index == length(pipeline), do: final_result, else: data),
          execution_time_ms: div(total_time, max(length(pipeline), 1)),
          operation_config: operation
        }
      end)

      %{
        success: true,
        original_data: data,
        final_result: final_result,
        total_execution_time_ms: total_time,
        steps: steps,
        summary: create_summary(steps, data, final_result)
      }
    rescue
      error ->
        end_time = System.monotonic_time(:millisecond)

        %{
          success: false,
          error: Exception.message(error),
          error_type: error.__struct__ |> Module.split() |> List.last(),
          total_execution_time_ms: end_time - start_time,
          original_data: data
        }
    end
  end

  # Private helper functions

  defp create_operation_description(%{"op" => "drop", "paths" => paths}) do
    "Dropping fields: #{Enum.join(paths, ", ")}"
  end

  defp create_operation_description(%{"op" => "project", "paths" => paths}) do
    "Projecting fields: #{Enum.join(paths, ", ")}"
  end

  defp create_operation_description(%{"op" => "rename", "mapping" => mapping}) do
    renames = Enum.map(mapping, fn {from, to} -> "#{from} → #{to}" end)
    "Renaming fields: #{Enum.join(renames, ", ")}"
  end

  defp create_operation_description(%{"op" => "list_to_map", "list_path" => list_path, "key_field" => key_field}) do
    "Converting list at '#{list_path}' to map using key '#{key_field}'"
  end

  defp create_operation_description(%{"op" => "prune"}) do
    "Pruning empty/null values"
  end

  defp create_operation_description(%{"op" => "set", "path" => path, "value" => value}) do
    "Setting '#{path}' to #{inspect(value)}"
  end

  defp create_operation_description(%{"op" => "copy", "from_path" => from_path, "to_path" => to_path}) do
    "Copying from '#{from_path}' to '#{to_path}'"
  end

  defp create_operation_description(%{"op" => "move", "from_path" => from_path, "to_path" => to_path}) do
    "Moving from '#{from_path}' to '#{to_path}'"
  end

  defp create_operation_description(%{"op" => "transform", "path" => path, "function" => function}) do
    "Transforming '#{path}' using function '#{function}'"
  end

  defp create_operation_description(%{"op" => "merge", "target_path" => target_path, "strategy" => strategy}) do
    "Merging data into '#{target_path}' using strategy '#{strategy}'"
  end

  defp create_operation_description(operation) do
    "Unknown operation: #{operation["op"] || "unspecified"}"
  end



  defp create_summary(steps, original_data, final_result) do
    original_size = data_size(original_data)
    final_size = data_size(final_result)

    size_reduction = if original_size > 0 do
      reduction_percent = ((original_size - final_size) / original_size * 100)
      if reduction_percent >= 0 do
        "-#{Float.round(reduction_percent, 1)}%"
      else
        "+#{Float.round(abs(reduction_percent), 1)}%"
      end
    else
      "0.0%"
    end

    %{
      total_steps: length(steps),
      operations_executed: length(steps),
      data_size_reduction: size_reduction,
      original_data_size: original_size,
      final_data_size: final_size
    }
  end

  defp data_size(data) when is_map(data), do: map_size(data) + Enum.sum(Enum.map(data, fn {_k, v} -> data_size(v) end))
  defp data_size(data) when is_list(data), do: length(data) + Enum.sum(Enum.map(data, &data_size/1))
  defp data_size(data) when is_binary(data), do: String.length(data)
  defp data_size(_), do: 1
end
