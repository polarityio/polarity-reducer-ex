defmodule DslInterpreter do
  @moduledoc """
  A JSON-based Domain Specific Language (DSL) interpreter for data transformation and reduction.

  The interpreter takes two arguments:
  - `data_map`: The raw, top-level data map (with string keys)
  - `dsl_map`: The DSL configuration map (with string keys) defining transformations

  The interpreter processes three main sections:
  1. `root`: Defines the initial working object
  2. `pipeline`: Array of transformation operations
  3. `output`: Final object projection with $root and $working variables
  """

  @doc """
  Main entry point for the DSL interpreter.

  ## Parameters
  - `data_map`: The original data map to transform
  - `dsl_map`: The DSL configuration defining the transformations

  ## Returns
  The transformed data according to the DSL specification

  ## Example
      iex> data = %{"details" => %{"name" => "test"}, "summary" => "info"}
      iex> dsl = %{
      ...>   "root" => %{"path" => "details", "on_null" => "return_empty"},
      ...>   "pipeline" => [],
      ...>   "output" => %{"name" => "$working.name", "info" => "$root.summary"}
      ...> }
      iex> DslInterpreter.execute(data, dsl)
      %{"name" => "test", "info" => "info"}
  """
  def execute(data_map, dsl_map) when is_map(data_map) and is_map(dsl_map) do
    # Step 1: Resolve the root object
    {working_map, original_data} = resolve_root_object(data_map, dsl_map["root"])

    # Step 2: Execute the pipeline
    transformed_working = execute_pipeline(working_map, dsl_map["pipeline"] || [])

    # Step 3: Build the output object
    build_output_object(original_data, transformed_working, dsl_map["output"] || %{})
  end

  # Handles the root object resolution with path and on_null logic
  defp resolve_root_object(data_map, root_config) when is_map(root_config) do
    path = root_config["path"]
    on_null = root_config["on_null"] || "return_empty"

    working_map = case get_nested_value(data_map, parse_path(path)) do
      nil ->
        case on_null do
          "return_original" -> data_map
          _ -> %{}
        end
      value -> value
    end

    {working_map, data_map}
  end

  defp resolve_root_object(data_map, _), do: {data_map, data_map}

  # Executes the pipeline of operations sequentially
  defp execute_pipeline(working_map, pipeline) when is_list(pipeline) do
    Enum.reduce(pipeline, working_map, &apply_operation(&2, &1))
  end

  defp execute_pipeline(working_map, _), do: working_map

  # Central dispatcher for all operations
  defp apply_operation(working_map, %{"op" => "drop"} = operation) do
    apply_drop_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "project"} = operation) do
    apply_project_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "project_and_replace"} = operation) do
    apply_project_and_replace_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "hoist_map_values"} = operation) do
    apply_hoist_map_values_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "list_to_map"} = operation) do
    apply_list_to_map_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "list_to_dynamic_map"} = operation) do
    apply_list_to_dynamic_map_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "promote_list_to_keys"} = operation) do
    apply_promote_list_to_keys_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "truncate_list"} = operation) do
    apply_truncate_list_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "aggregate_list"} = operation) do
    apply_aggregate_list_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "prune"} = operation) do
    apply_prune_operation(working_map, operation)
  end

  defp apply_operation(working_map, _unknown_operation), do: working_map

  # ===== OPERATION IMPLEMENTATIONS =====

  # Drop operation: removes specified paths from the working map
  defp apply_drop_operation(working_map, %{"paths" => paths}) when is_list(paths) do
    Enum.reduce(paths, working_map, fn path, acc ->
      drop_at_path(acc, parse_path(path))
    end)
  end

  defp apply_drop_operation(working_map, _), do: working_map

  # Project operation: creates a new structure at a specific path
  defp apply_project_operation(working_map, %{"path" => path, "mapping" => mapping}) do
    source_data = get_nested_value(working_map, parse_path(path))
    projected_data = project_data(source_data, mapping)
    put_nested_value(working_map, parse_path(path), projected_data)
  end

  defp apply_project_operation(working_map, _), do: working_map

  # Project and replace operation: replaces the entire working map with a projection
  defp apply_project_and_replace_operation(working_map, %{"projection" => projection}) do
    project_data(working_map, projection)
  end

  defp apply_project_and_replace_operation(working_map, _), do: working_map

  # Hoist map values operation: moves values from a nested map to parent level
  defp apply_hoist_map_values_operation(working_map, %{"path" => path, "child_key" => child_key} = operation) do
    replace_parent = Map.get(operation, "replace_parent", false)

    update_at_path(working_map, parse_path(path), fn target_map ->
      case target_map do
        %{^child_key => child_map} when is_map(child_map) ->
          if replace_parent do
            # Merge child map keys into parent and remove the child
            target_map
            |> Map.delete(child_key)
            |> Map.merge(child_map)
          else
            Map.put(target_map, child_key, child_map)
          end
        _ -> target_map
      end
    end)
  end

  defp apply_hoist_map_values_operation(working_map, _), do: working_map

  # List to map operation: converts a list of key-value objects to a map
  defp apply_list_to_map_operation(working_map, %{"path" => path, "key_from" => key_from, "value_from" => value_from}) do
    update_at_path(working_map, parse_path(path), fn target ->
      case target do
        list when is_list(list) ->
          Enum.reduce(list, %{}, fn item, acc ->
            case item do
              %{^key_from => key, ^value_from => value} when is_binary(key) ->
                Map.put(acc, key, value)
              _ -> acc
            end
          end)
        _ -> target
      end
    end)
  end

  defp apply_list_to_map_operation(working_map, _), do: working_map

  # List to dynamic map operation: similar to list_to_map but groups by key_from
  defp apply_list_to_dynamic_map_operation(working_map, %{"path" => path, "key_from" => key_from, "value_from" => value_from}) do
    update_at_path(working_map, parse_path(path), fn target ->
      case target do
        list when is_list(list) ->
          list
          |> Enum.group_by(fn item -> Map.get(item, key_from) end)
          |> Enum.reduce(%{}, fn {key, items}, acc ->
            values = Enum.map(items, fn item -> Map.get(item, value_from) end)
            Map.put(acc, key, values)
          end)
        _ -> target
      end
    end)
  end

  defp apply_list_to_dynamic_map_operation(working_map, _), do: working_map

  # Promote list to keys operation: converts a list to key-value pairs at parent level
  defp apply_promote_list_to_keys_operation(working_map, %{"path" => path, "child_list" => child_list, "key_from" => key_from, "value_from" => value_from}) do
    update_at_path(working_map, parse_path(path), fn target_map ->
      case Map.get(target_map, child_list) do
        list when is_list(list) ->
          promoted_map = Enum.reduce(list, %{}, fn item, acc ->
            case item do
              %{^key_from => key, ^value_from => value} when is_binary(key) ->
                Map.put(acc, key, value)
              _ -> acc
            end
          end)

          target_map
          |> Map.delete(child_list)
          |> Map.merge(promoted_map)
        _ -> target_map
      end
    end)
  end

  defp apply_promote_list_to_keys_operation(working_map, _), do: working_map

  # Truncate list operation: limits list size and provides metadata
  defp apply_truncate_list_operation(working_map, %{"path" => path, "max_size" => max_size, "shape" => shape}) do
    update_at_path(working_map, parse_path(path), fn target ->
      case target do
        list when is_list(list) ->
          process_truncate_shape(shape, list, max_size)
        _ -> target
      end
    end)
  end

  defp apply_truncate_list_operation(working_map, _), do: working_map

  # Aggregate list operation: computes aggregations over list fields
  defp apply_aggregate_list_operation(working_map, %{"path" => path, "shape" => shape}) do
    update_at_path(working_map, parse_path(path), fn target ->
      case target do
        list when is_list(list) ->
          process_aggregate_shape(shape, list)
        _ -> target
      end
    end)
  end

  defp apply_aggregate_list_operation(working_map, _), do: working_map

  # Prune operation: removes empty values recursively
  defp apply_prune_operation(working_map, %{"strategy" => "empty_values"}) do
    prune_empty_values(working_map)
  end

  defp apply_prune_operation(working_map, _), do: working_map

  # ===== PATH UTILITIES =====

  # Parse a string path into a list of components, handling [] wildcards
  defp parse_path(nil), do: []
  defp parse_path(""), do: []
  defp parse_path(path) when is_binary(path) do
    path
    |> String.split(".")
    |> Enum.flat_map(fn segment ->
      case String.contains?(segment, "[]") do
        true -> String.split(segment, "[]", trim: true) ++ ["[]"]
        false -> [segment]
      end
    end)
    |> Enum.reject(&(&1 == ""))
  end

  # Get a nested value using a parsed path with wildcard support
  defp get_nested_value(map, []), do: map
  defp get_nested_value(map, [key | rest]) when is_map(map) do
    case Map.get(map, key) do
      nil -> nil
      value -> get_nested_value(value, rest)
    end
  end
  defp get_nested_value(list, ["[]" | rest]) when is_list(list) do
    Enum.map(list, &get_nested_value(&1, rest))
  end
  defp get_nested_value(_, _), do: nil

  # Put a nested value using a parsed path with wildcard support
  defp put_nested_value(_map, [], value), do: value
  defp put_nested_value(map, [key], value) when is_map(map) do
    Map.put(map, key, value)
  end
  defp put_nested_value(map, [key | rest], value) when is_map(map) do
    current = Map.get(map, key, %{})
    Map.put(map, key, put_nested_value(current, rest, value))
  end
  defp put_nested_value(list, ["[]" | rest], value) when is_list(list) do
    Enum.map(list, &put_nested_value(&1, rest, value))
  end
  defp put_nested_value(data, _, _), do: data

  # Update a nested value using a function with wildcard support
  defp update_at_path(map, [], func), do: func.(map)
  defp update_at_path(map, [key], func) when is_map(map) do
    Map.update(map, key, nil, func)
  end
  defp update_at_path(map, [key | rest], func) when is_map(map) do
    current = Map.get(map, key, %{})
    Map.put(map, key, update_at_path(current, rest, func))
  end
  defp update_at_path(list, ["[]" | rest], func) when is_list(list) do
    Enum.map(list, &update_at_path(&1, rest, func))
  end
  defp update_at_path(data, _, _), do: data

  # Drop a value at a specific path with wildcard support
  defp drop_at_path(_map, []), do: %{}
  defp drop_at_path(map, [key]) when is_map(map) do
    Map.delete(map, key)
  end
  defp drop_at_path(map, [key | rest]) when is_map(map) do
    case Map.get(map, key) do
      nil -> map
      value -> Map.put(map, key, drop_at_path(value, rest))
    end
  end
  defp drop_at_path(list, ["[]" | rest]) when is_list(list) do
    Enum.map(list, &drop_at_path(&1, rest))
  end
  defp drop_at_path(data, _), do: data

  # ===== HELPER FUNCTIONS =====

  # Project data using a mapping configuration
  defp project_data(source_data, mapping) when is_map(mapping) do
    Enum.reduce(mapping, %{}, fn {new_key, old_path}, acc ->
      value = get_nested_value(source_data, parse_path(old_path))
      Map.put(acc, new_key, value)
    end)
  end

  defp project_data(source_data, _), do: source_data

  # Process truncate list shape with special variables
  defp process_truncate_shape(shape, list, _max_size) when is_map(shape) do
    length = length(list)

    Enum.reduce(shape, %{}, fn {key, value}, acc ->
      resolved_value = case value do
        "$length" -> length
        "$slice(" <> rest ->
          case parse_slice_params(rest) do
            {start_idx, end_idx} -> Enum.slice(list, start_idx, end_idx - start_idx)
            _ -> nil
          end
        "$map_slice(" <> rest ->
          case parse_map_slice_params(rest) do
            {start_idx, end_idx, path} ->
              list
              |> Enum.slice(start_idx, end_idx - start_idx)
              |> Enum.map(&get_nested_value(&1, parse_path(path)))
            _ -> nil
          end
        _ -> value
      end
      Map.put(acc, key, resolved_value)
    end)
  end

  defp process_truncate_shape(_, list, _), do: list

  # Process aggregate list shape with special variables
  defp process_aggregate_shape(shape, list) when is_map(shape) do
    Enum.reduce(shape, %{}, fn {key, value}, acc ->
      resolved_value = case value do
        "$min(" <> field_path ->
          field = String.trim_trailing(field_path, ")")
          list
          |> Enum.map(&get_nested_value(&1, parse_path(field)))
          |> Enum.reject(&is_nil/1)
          |> Enum.min(fn -> nil end)
        "$max(" <> field_path ->
          field = String.trim_trailing(field_path, ")")
          list
          |> Enum.map(&get_nested_value(&1, parse_path(field)))
          |> Enum.reject(&is_nil/1)
          |> Enum.max(fn -> nil end)
        _ -> value
      end
      Map.put(acc, key, resolved_value)
    end)
  end

  defp process_aggregate_shape(_, list), do: list

  # Parse slice parameters from string like "0, 5)"
  defp parse_slice_params(params_str) do
    case String.trim_trailing(params_str, ")") |> String.split(",") do
      [start_str, end_str] ->
        with {start_idx, ""} <- Integer.parse(String.trim(start_str)),
             {end_idx, ""} <- Integer.parse(String.trim(end_str)) do
          {start_idx, end_idx}
        else
          _ -> nil
        end
      _ -> nil
    end
  end

  # Parse map slice parameters from string like "0, 5, field.name)"
  defp parse_map_slice_params(params_str) do
    case String.trim_trailing(params_str, ")") |> String.split(",") do
      [start_str, end_str, path_str] ->
        with {start_idx, ""} <- Integer.parse(String.trim(start_str)),
             {end_idx, ""} <- Integer.parse(String.trim(end_str)) do
          {start_idx, end_idx, String.trim(path_str)}
        else
          _ -> nil
        end
      _ -> nil
    end
  end

  # Recursively remove empty values (nil, "", {}, [])
  defp prune_empty_values(map) when is_map(map) do
    map
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      pruned_value = prune_empty_values(value)
      if is_empty_value?(pruned_value) do
        acc
      else
        Map.put(acc, key, pruned_value)
      end
    end)
  end

  defp prune_empty_values(list) when is_list(list) do
    list
    |> Enum.map(&prune_empty_values/1)
    |> Enum.reject(&is_empty_value?/1)
  end

  defp prune_empty_values(value), do: value

  # Check if a value is considered empty for pruning
  defp is_empty_value?(nil), do: true
  defp is_empty_value?(""), do: true
  defp is_empty_value?(map) when map == %{}, do: true
  defp is_empty_value?(list) when list == [], do: true
  defp is_empty_value?(_), do: false

  # Build the final output object with $root and $working variable resolution
  defp build_output_object(original_data, working_data, output_spec) when is_map(output_spec) do
    Enum.reduce(output_spec, %{}, fn {key, value_spec}, acc ->
      resolved_value = resolve_output_variable(value_spec, original_data, working_data)
      Map.put(acc, key, resolved_value)
    end)
  end

  defp build_output_object(_, working_data, _), do: working_data

  # Resolve special variables in output specification
  defp resolve_output_variable("$root" <> rest, original_data, _working_data) do
    case rest do
      "" -> original_data
      "." <> path -> get_nested_value(original_data, parse_path(path))
      _ -> nil
    end
  end

  defp resolve_output_variable("$working" <> rest, _original_data, working_data) do
    case rest do
      "" -> working_data
      "." <> path -> get_nested_value(working_data, parse_path(path))
      _ -> nil
    end
  end

  defp resolve_output_variable(literal_value, _original_data, _working_data) do
    literal_value
  end

  # ===== PUBLIC WRAPPERS FOR TESTING SDK =====

  @doc false
  def resolve_root_object_public(data_map, root_config) do
    resolve_root_object(data_map, root_config)
  end

  @doc false
  def build_output_object_public(original_data, working_data, output_spec) do
    build_output_object(original_data, working_data, output_spec)
  end

  @doc false
  def apply_drop_operation_public(working_map, operation) do
    apply_drop_operation(working_map, operation)
  end

  @doc false
  def apply_project_operation_public(working_map, operation) do
    apply_project_operation(working_map, operation)
  end

  @doc false
  def apply_project_and_replace_operation_public(working_map, operation) do
    apply_project_and_replace_operation(working_map, operation)
  end

  @doc false
  def apply_hoist_map_values_operation_public(working_map, operation) do
    apply_hoist_map_values_operation(working_map, operation)
  end

  @doc false
  def apply_list_to_map_operation_public(working_map, operation) do
    apply_list_to_map_operation(working_map, operation)
  end

  @doc false
  def apply_list_to_dynamic_map_operation_public(working_map, operation) do
    apply_list_to_dynamic_map_operation(working_map, operation)
  end

  @doc false
  def apply_promote_list_to_keys_operation_public(working_map, operation) do
    apply_promote_list_to_keys_operation(working_map, operation)
  end

  @doc false
  def apply_truncate_list_operation_public(working_map, operation) do
    apply_truncate_list_operation(working_map, operation)
  end

  @doc false
  def apply_aggregate_list_operation_public(working_map, operation) do
    apply_aggregate_list_operation(working_map, operation)
  end

  @doc false
  def apply_prune_operation_public(working_map, operation) do
    apply_prune_operation(working_map, operation)
  end
end
