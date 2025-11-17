defmodule PolarityReducerEx.DslInterpreter do
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

  defp apply_operation(working_map, %{"op" => "rename"} = operation) do
    apply_rename_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "format_date"} = operation) do
    apply_format_date_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "parse_date"} = operation) do
    apply_parse_date_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "date_add"} = operation) do
    apply_date_add_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "date_diff"} = operation) do
    apply_date_diff_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "current_timestamp"} = operation) do
    apply_current_timestamp_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "set"} = operation) do
    apply_set_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "transform"} = operation) do
    apply_transform_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "copy"} = operation) do
    apply_copy_operation(working_map, operation)
  end

  defp apply_operation(working_map, %{"op" => "move"} = operation) do
    apply_move_operation(working_map, operation)
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



  # Handle array-to-array copying specially
  defp apply_set_operation(working_map, %{"path" => path, "value" => "$path:" <> source_path}) do
    target_parsed = parse_path(path)
    source_parsed = parse_path(source_path)

    # Check if both paths are array paths with the same structure
    case {target_parsed, source_parsed} do
      # Both are array paths like "users[].display_name" and "users[].first_name"
      {[array_name, "[]" | target_fields], [same_array_name, "[]" | source_fields]}
      when array_name == same_array_name ->
        # Handle element-wise copying within the same array
        apply_array_element_copying(working_map, array_name, target_fields, source_fields)

      _ ->
        # Regular path resolution
        update_at_path(working_map, target_parsed, fn _ ->
          resolve_set_value(source_path, working_map)
        end)
    end
  end

  # Handle static values and other cases
  defp apply_set_operation(working_map, %{"path" => path, "value" => value}) do
    update_at_path(working_map, parse_path(path), fn _ -> value end)
  end

  defp apply_set_operation(working_map, _), do: working_map

  # Handle element-wise copying within arrays
  defp apply_array_element_copying(working_map, array_name, target_fields, source_fields) do
    case Map.get(working_map, array_name) do
      list when is_list(list) ->
        updated_list = Enum.map(list, fn element ->
          case get_nested_value(element, source_fields) do
            nil -> element
            value -> update_at_path(element, target_fields, fn _ -> value end)
          end
        end)
        Map.put(working_map, array_name, updated_list)

      _ ->
        working_map
    end
  end

  # Regular resolve function for non-array cases
  defp resolve_set_value(source_path, working_map) do
    get_nested_value(working_map, parse_path(source_path))
  end

  # Rename operation: renames fields according to mapping
  defp apply_rename_operation(working_map, %{"mapping" => mapping}) when is_map(mapping) do
    Enum.reduce(mapping, working_map, fn {from_path, to_path}, acc ->
      rename_field(acc, from_path, to_path)
    end)
  end

  defp apply_rename_operation(working_map, _), do: working_map

  # Format date operation: formats date/datetime fields
  defp apply_format_date_operation(working_map, %{"path" => path, "format" => format} = operation) do
    input_format = Map.get(operation, "input_format", "auto")

    update_at_path(working_map, parse_path(path), fn value ->
      format_date_value(value, input_format, format)
    end)
  end

  defp apply_format_date_operation(working_map, _), do: working_map

  # Parse date operation: parses date strings into standardized format
  defp apply_parse_date_operation(working_map, %{"path" => path} = operation) do
    input_format = Map.get(operation, "input_format", "auto")
    output_format = Map.get(operation, "output_format", "iso8601")

    update_at_path(working_map, parse_path(path), fn value ->
      parse_date_value(value, input_format, output_format)
    end)
  end

  defp apply_parse_date_operation(working_map, _), do: working_map

  # Date add operation: adds time intervals to date fields
  defp apply_date_add_operation(working_map, %{"path" => path, "amount" => amount, "unit" => unit} = operation) do
    input_format = Map.get(operation, "input_format", "auto")
    output_format = Map.get(operation, "output_format", "iso8601")

    update_at_path(working_map, parse_path(path), fn value ->
      add_to_date_value(value, amount, unit, input_format, output_format)
    end)
  end

  defp apply_date_add_operation(working_map, _), do: working_map

  # Date diff operation: calculates difference between two date fields
  defp apply_date_diff_operation(working_map, %{"from_path" => from_path, "to_path" => to_path, "result_path" => result_path} = operation) do
    unit = Map.get(operation, "unit", "days")
    input_format = Map.get(operation, "input_format", "auto")

    from_value = get_nested_value(working_map, parse_path(from_path))
    to_value = get_nested_value(working_map, parse_path(to_path))

    diff_result = calculate_date_diff(from_value, to_value, unit, input_format)

    put_nested_value(working_map, parse_path(result_path), diff_result)
  end

  defp apply_date_diff_operation(working_map, _), do: working_map

  # Current timestamp operation: adds current timestamp to specified path
  defp apply_current_timestamp_operation(working_map, %{"path" => path} = operation) do
    format = Map.get(operation, "format", "iso8601")
    timezone = Map.get(operation, "timezone", "UTC")

    timestamp = generate_current_timestamp(format, timezone)
    put_nested_value(working_map, parse_path(path), timestamp)
  end

  defp apply_current_timestamp_operation(working_map, _), do: working_map

  # Transform operation: applies transformation functions to field values
  defp apply_transform_operation(working_map, %{"path" => path, "function" => function} = operation) do
    args = Map.get(operation, "args", [])
    
    update_at_path(working_map, parse_path(path), fn value ->
      apply_transform_function(value, function, args)
    end)
  end

  defp apply_transform_operation(working_map, _), do: working_map

  # Apply transformation functions to values
  defp apply_transform_function(value, "uppercase", _args) when is_binary(value) do
    String.upcase(value)
  end

  defp apply_transform_function(value, "lowercase", _args) when is_binary(value) do
    String.downcase(value)
  end

  defp apply_transform_function(value, "capitalize", _args) when is_binary(value) do
    String.capitalize(value)
  end

  defp apply_transform_function(value, "trim", _args) when is_binary(value) do
    String.trim(value)
  end

  defp apply_transform_function(value, "string", _args) do
    cond do
      is_binary(value) -> value
      is_number(value) -> to_string(value)
      is_boolean(value) -> to_string(value)
      is_nil(value) -> ""
      is_atom(value) -> Atom.to_string(value)
      true -> inspect(value)
    end
  end

  defp apply_transform_function(value, "number", _args) when is_binary(value) do
    case Float.parse(value) do
      {num, ""} -> 
        # Check if it's actually an integer
        if trunc(num) == num do
          trunc(num)
        else
          num
        end
      {num, _} -> num
      :error -> 
        case Integer.parse(value) do
          {int, ""} -> int
          {int, _} -> int
          :error -> nil
        end
    end
  end

  defp apply_transform_function(value, "number", _args) when is_number(value), do: value
  defp apply_transform_function(_value, "number", _args), do: nil

  defp apply_transform_function(value, "integer", _args) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      {int, _} -> int
      :error -> nil
    end
  end

  defp apply_transform_function(value, "integer", _args) when is_integer(value), do: value
  defp apply_transform_function(value, "integer", _args) when is_float(value), do: trunc(value)
  defp apply_transform_function(_value, "integer", _args), do: nil

  defp apply_transform_function(value, "float", _args) when is_binary(value) do
    case Float.parse(value) do
      {float, ""} -> float
      {float, _} -> float
      :error -> nil
    end
  end

  defp apply_transform_function(value, "float", _args) when is_float(value), do: value
  defp apply_transform_function(value, "float", _args) when is_integer(value), do: value * 1.0
  defp apply_transform_function(_value, "float", _args), do: nil

  defp apply_transform_function(value, "boolean", _args) do
    case value do
      true -> true
      false -> false
      nil -> false
      "" -> false
      0 -> false
      0.0 -> false
      "false" -> false
      "False" -> false
      "FALSE" -> false
      "0" -> false
      _ -> true
    end
  end

  defp apply_transform_function(value, "length", _args) when is_binary(value) do
    String.length(value)
  end

  defp apply_transform_function(value, "length", _args) when is_list(value) do
    length(value)
  end

  defp apply_transform_function(value, "length", _args) when is_map(value) do
    map_size(value)
  end

  defp apply_transform_function(_value, "length", _args), do: nil

  defp apply_transform_function(value, "reverse", _args) when is_binary(value) do
    String.reverse(value)
  end

  defp apply_transform_function(value, "reverse", _args) when is_list(value) do
    Enum.reverse(value)
  end

  defp apply_transform_function(value, "reverse", _args), do: value

  defp apply_transform_function(value, "split", args) when is_binary(value) and is_list(args) do
    delimiter = List.first(args) || " "
    String.split(value, delimiter)
  end

  defp apply_transform_function(value, "split", _args), do: value

  defp apply_transform_function(value, "join", args) when is_list(value) and is_list(args) do
    delimiter = List.first(args) || " "
    Enum.join(value, delimiter)
  end

  defp apply_transform_function(value, "join", _args), do: value

  defp apply_transform_function(value, "abs", _args) when is_number(value) do
    abs(value)
  end

  defp apply_transform_function(value, "abs", _args), do: value

  defp apply_transform_function(value, "round", args) when is_number(value) do
    precision = case args do
      [p] when is_integer(p) -> p
      _ -> 0
    end
    Float.round(value * 1.0, precision)
  end

  defp apply_transform_function(value, "round", _args), do: value

  # Default case - return original value if function not recognized
  defp apply_transform_function(value, _function, _args), do: value

  # Copy operation: copies values from one path to another
  defp apply_copy_operation(working_map, %{"from" => from_path, "to" => to_path}) do
    from_parsed = parse_path(from_path)
    to_parsed = parse_path(to_path)
    
    # Check if both paths are array paths with the same structure for element-wise copying
    case {from_parsed, to_parsed} do
      # Both are array paths like "users[].email" and "users[].backup_email"
      {[array_name, "[]" | from_fields], [same_array_name, "[]" | to_fields]} 
      when array_name == same_array_name ->
        # Handle element-wise copying within the same array
        apply_array_element_copying(working_map, array_name, to_fields, from_fields)
      
      # Different arrays or regular paths
      _ ->
        # Get the source value
        source_value = get_nested_value(working_map, from_parsed)
        # Set it at the target path
        put_nested_value(working_map, to_parsed, source_value)
    end
  end

  defp apply_copy_operation(working_map, _), do: working_map

  # Move operation: moves values from one path to another (copy + drop in one atomic operation)
  defp apply_move_operation(working_map, %{"from" => from_path, "to" => to_path}) do
    from_parsed = parse_path(from_path)
    to_parsed = parse_path(to_path)
    
    # Check if both paths are array paths with the same structure for element-wise moving
    case {from_parsed, to_parsed} do
      # Both are array paths like "users[].old_email" and "users[].new_email"
      {[array_name, "[]" | from_fields], [same_array_name, "[]" | to_fields]} 
      when array_name == same_array_name ->
        # Handle element-wise moving within the same array
        apply_array_element_moving(working_map, array_name, from_fields, to_fields)
      
      # Different arrays or regular paths
      _ ->
        # Get the source value
        source_value = get_nested_value(working_map, from_parsed)
        # Set it at the target path
        updated_map = put_nested_value(working_map, to_parsed, source_value)
        # Remove it from the source path
        drop_at_path(updated_map, from_parsed)
    end
  end

  defp apply_move_operation(working_map, _), do: working_map

  # Handle element-wise moving within arrays (copy then drop source fields)  
  defp apply_array_element_moving(working_map, array_name, from_fields, to_fields) do
    case Map.get(working_map, array_name) do
      list when is_list(list) ->
        updated_list = Enum.map(list, fn element ->
          case get_nested_value(element, from_fields) do
            nil -> element
            value -> 
              # Copy to new location and drop from old location
              element
              |> update_at_path(to_fields, fn _ -> value end)
              |> drop_at_path(from_fields)
          end
        end)
        Map.put(working_map, array_name, updated_list)

      _ ->
        working_map
    end
  end

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
    Map.update(map, key, func.(nil), func)
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

  # Rename a field from one path to another with wildcard support
  defp rename_field(data, from_path, to_path) do
    from_parsed = parse_path(from_path)
    to_parsed = parse_path(to_path)

    # Find the common prefix and divergent suffix
    {common_prefix, from_suffix, to_suffix} = find_path_divergence(from_parsed, to_parsed)

    # Navigate to the common prefix and perform the rename on the divergent parts
    rename_with_prefix(data, common_prefix, from_suffix, to_suffix)
  end

  # Find where two paths diverge
  defp find_path_divergence(from_path, to_path) do
    find_path_divergence(from_path, to_path, [])
  end

  defp find_path_divergence([same | from_rest], [same | to_rest], common_acc) do
    find_path_divergence(from_rest, to_rest, common_acc ++ [same])
  end

  defp find_path_divergence(from_rest, to_rest, common_acc) do
    {common_acc, from_rest, to_rest}
  end

  # Rename with a common prefix - navigate to the prefix then rename the suffixes
  defp rename_with_prefix(data, [], from_suffix, to_suffix) do
    # We're at the divergence point, do the actual rename
    perform_suffix_rename(data, from_suffix, to_suffix)
  end

  defp rename_with_prefix(data, [key | prefix_rest], from_suffix, to_suffix) when is_map(data) do
    case Map.get(data, key) do
      nil -> data
      value -> Map.put(data, key, rename_with_prefix(value, prefix_rest, from_suffix, to_suffix))
    end
  end

  defp rename_with_prefix(list, ["[]" | prefix_rest], from_suffix, to_suffix) when is_list(list) do
    Enum.map(list, &rename_with_prefix(&1, prefix_rest, from_suffix, to_suffix))
  end

  defp rename_with_prefix(data, _, _, _), do: data

  # Perform the actual rename at the divergence point
  defp perform_suffix_rename(data, [from_key], [to_key]) when is_map(data) do
    case Map.get(data, from_key) do
      nil -> data
      value ->
        data
        |> Map.put(to_key, value)
        |> Map.delete(from_key)
    end
  end

  defp perform_suffix_rename(data, from_suffix, to_suffix) when is_map(data) do
    # Handle multi-level suffixes recursively
    case {from_suffix, to_suffix} do
      {[from_key | from_rest], [to_key | to_rest]} ->
        case Map.get(data, from_key) do
          nil -> data
          value ->
            renamed_value = perform_suffix_rename(value, from_rest, to_rest)
            data
            |> Map.put(to_key, renamed_value)
            |> Map.delete(from_key)
        end
      _ -> data
    end
  end

  defp perform_suffix_rename(list, ["[]" | from_rest], ["[]" | to_rest]) when is_list(list) do
    Enum.map(list, &perform_suffix_rename(&1, from_rest, to_rest))
  end

  defp perform_suffix_rename(data, _, _), do: data

  # ===== HELPER FUNCTIONS =====

  # Resolve a set value - can be a static value or a path reference


  # Project data using a mapping configuration
  defp project_data(source_data, mapping) when is_list(source_data) and is_map(mapping) do
    Enum.map(source_data, fn item -> project_data(item, mapping) end)
  end

  defp project_data(source_data, mapping) when is_map(source_data) and is_map(mapping) do
    Enum.reduce(mapping, %{}, fn {new_key, old_path}, acc ->
      value = get_nested_value(source_data, parse_path(old_path))
      Map.put(acc, new_key, value)
    end)
  end

  defp project_data(source_data, _), do: source_data

  # ===== DATE/TIME HELPER FUNCTIONS =====

  # Format a date value according to the specified format
  defp format_date_value(value, input_format, output_format) when is_binary(value) do
    case parse_datetime_string(value, input_format) do
      {:ok, datetime} -> format_datetime(datetime, output_format)
      {:error, _} -> value  # Return original if parsing fails
    end
  end

  defp format_date_value(value, _input_format, _output_format), do: value

  # Parse a date value into standardized format
  defp parse_date_value(value, input_format, output_format) when is_binary(value) do
    case parse_datetime_string(value, input_format) do
      {:ok, datetime} -> format_datetime(datetime, output_format)
      {:error, _} -> value  # Return original if parsing fails
    end
  end

  defp parse_date_value(value, _input_format, _output_format), do: value

  # Add time interval to a date value
  defp add_to_date_value(value, amount, unit, input_format, output_format) when is_binary(value) do
    case parse_datetime_string(value, input_format) do
      {:ok, datetime} ->
        new_datetime = add_time_to_datetime(datetime, amount, unit)
        format_datetime(new_datetime, output_format)
      {:error, _} -> value  # Return original if parsing fails
    end
  end

  defp add_to_date_value(value, _amount, _unit, _input_format, _output_format), do: value

  # Calculate difference between two dates
  defp calculate_date_diff(from_value, to_value, unit, input_format) when is_binary(from_value) and is_binary(to_value) do
    with {:ok, from_datetime} <- parse_datetime_string(from_value, input_format),
         {:ok, to_datetime} <- parse_datetime_string(to_value, input_format) do
      calculate_datetime_diff(from_datetime, to_datetime, unit)
    else
      _ -> nil  # Return nil if parsing fails
    end
  end

  defp calculate_date_diff(_from_value, _to_value, _unit, _input_format), do: nil

  # Generate current timestamp
  defp generate_current_timestamp(format, timezone) do
    now = DateTime.utc_now()

    # Convert to specified timezone if not UTC
    datetime = case timezone do
      "UTC" -> now
      tz ->
        case DateTime.shift_zone(now, tz) do
          {:ok, shifted} -> shifted
          {:error, _} -> now  # Fallback to UTC if timezone is invalid
        end
    end

    format_datetime(datetime, format)
  end

  # Parse datetime string with auto-detection or specific format
  defp parse_datetime_string(value, "auto") do
    # Try common formats in order
    formats = [
      # ISO 8601 formats
      ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/,
      ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/,
      ~r/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/,
      ~r/^\d{4}-\d{2}-\d{2}$/,
      # US formats
      ~r/^\d{1,2}\/\d{1,2}\/\d{4}$/,
      ~r/^\d{1,2}-\d{1,2}-\d{4}$/,
      # Unix timestamp
      ~r/^\d{10}$/,
      ~r/^\d{13}$/
    ]

    cond do
      Regex.match?(~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/, value) ->
        case DateTime.from_iso8601(value) do
          {:ok, dt, _} -> {:ok, dt}
          {:error, _} ->
            case NaiveDateTime.from_iso8601(value) do
              {:ok, ndt} -> {:ok, DateTime.from_naive!(ndt, "Etc/UTC")}
              {:error, reason} -> {:error, reason}
            end
        end

      Regex.match?(~r/^\d{4}-\d{2}-\d{2}$/, value) ->
        case Date.from_iso8601(value) do
          {:ok, date} -> {:ok, DateTime.new!(date, ~T[00:00:00], "Etc/UTC")}
          {:error, reason} -> {:error, reason}
        end

      Regex.match?(~r/^\d{10}$/, value) ->
        # Unix timestamp (seconds)
        case Integer.parse(value) do
          {timestamp, ""} -> {:ok, DateTime.from_unix!(timestamp)}
          _ -> {:error, :invalid_unix_timestamp}
        end

      Regex.match?(~r/^\d{13}$/, value) ->
        # Unix timestamp (milliseconds)
        case Integer.parse(value) do
          {timestamp, ""} -> {:ok, DateTime.from_unix!(timestamp, :millisecond)}
          _ -> {:error, :invalid_unix_timestamp}
        end

      true -> {:error, :unsupported_format}
    end
  end

  defp parse_datetime_string(value, format) do
    # For specific formats, we'd need more sophisticated parsing
    # For now, fall back to auto-detection
    parse_datetime_string(value, "auto")
  end

  # Format datetime according to specified format
  defp format_datetime(datetime, "iso8601"), do: DateTime.to_iso8601(datetime)
  defp format_datetime(datetime, "iso8601_basic"), do: DateTime.to_iso8601(datetime, :basic)
  defp format_datetime(datetime, "date_only"), do: DateTime.to_date(datetime) |> Date.to_iso8601()
  defp format_datetime(datetime, "time_only"), do: DateTime.to_time(datetime) |> Time.to_iso8601()
  defp format_datetime(datetime, "unix"), do: DateTime.to_unix(datetime) |> Integer.to_string()
  defp format_datetime(datetime, "unix_ms"), do: DateTime.to_unix(datetime, :millisecond) |> Integer.to_string()
  defp format_datetime(datetime, "human"), do: Calendar.strftime(datetime, "%Y-%m-%d %H:%M:%S %Z")
  defp format_datetime(datetime, _format), do: DateTime.to_iso8601(datetime)  # Default fallback

  # Add time to datetime
  defp add_time_to_datetime(datetime, amount, "seconds") do
    DateTime.add(datetime, amount, :second)
  end

  defp add_time_to_datetime(datetime, amount, "minutes") do
    DateTime.add(datetime, amount * 60, :second)
  end

  defp add_time_to_datetime(datetime, amount, "hours") do
    DateTime.add(datetime, amount * 3600, :second)
  end

  defp add_time_to_datetime(datetime, amount, "days") do
    DateTime.add(datetime, amount * 86400, :second)
  end

  defp add_time_to_datetime(datetime, amount, "weeks") do
    DateTime.add(datetime, amount * 604800, :second)
  end

  defp add_time_to_datetime(datetime, amount, "months") do
    # Approximate month as 30 days
    DateTime.add(datetime, amount * 2592000, :second)
  end

  defp add_time_to_datetime(datetime, amount, "years") do
    # Approximate year as 365 days
    DateTime.add(datetime, amount * 31536000, :second)
  end

  defp add_time_to_datetime(datetime, _amount, _unit), do: datetime

  # Calculate difference between datetimes
  defp calculate_datetime_diff(from_datetime, to_datetime, "seconds") do
    DateTime.diff(to_datetime, from_datetime, :second)
  end

  defp calculate_datetime_diff(from_datetime, to_datetime, "minutes") do
    DateTime.diff(to_datetime, from_datetime, :second) / 60
  end

  defp calculate_datetime_diff(from_datetime, to_datetime, "hours") do
    DateTime.diff(to_datetime, from_datetime, :second) / 3600
  end

  defp calculate_datetime_diff(from_datetime, to_datetime, "days") do
    DateTime.diff(to_datetime, from_datetime, :second) / 86400
  end

  defp calculate_datetime_diff(from_datetime, to_datetime, "weeks") do
    DateTime.diff(to_datetime, from_datetime, :second) / 604800
  end

  defp calculate_datetime_diff(from_datetime, to_datetime, _unit) do
    # Default to days
    DateTime.diff(to_datetime, from_datetime, :second) / 86400
  end

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

  defp resolve_output_variable(value_spec, original_data, working_data) when is_map(value_spec) do
    Enum.reduce(value_spec, %{}, fn {key, value}, acc ->
      resolved_value = resolve_output_variable(value, original_data, working_data)
      Map.put(acc, key, resolved_value)
    end)
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
  def apply_set_operation_public(working_map, operation) do
    apply_set_operation(working_map, operation)
  end

  @doc false
  def apply_prune_operation_public(working_map, operation) do
    apply_prune_operation(working_map, operation)
  end

  @doc false
  def apply_rename_operation_public(working_map, operation) do
    apply_rename_operation(working_map, operation)
  end

  @doc false
  def apply_format_date_operation_public(working_map, operation) do
    apply_format_date_operation(working_map, operation)
  end

  @doc false
  def apply_parse_date_operation_public(working_map, operation) do
    apply_parse_date_operation(working_map, operation)
  end

  @doc false
  def apply_date_add_operation_public(working_map, operation) do
    apply_date_add_operation(working_map, operation)
  end

  @doc false
  def apply_date_diff_operation_public(working_map, operation) do
    apply_date_diff_operation(working_map, operation)
  end

  @doc false
  def apply_current_timestamp_operation_public(working_map, operation) do
    apply_current_timestamp_operation(working_map, operation)
  end

  @doc false
  def apply_transform_operation_public(working_map, operation) do
    apply_transform_operation(working_map, operation)
  end

  @doc false
  def apply_copy_operation_public(working_map, operation) do
    apply_copy_operation(working_map, operation)
  end

  @doc false
  def apply_move_operation_public(working_map, operation) do
    apply_move_operation(working_map, operation)
  end

  @doc false
  def apply_operation_public(working_map, operation) do
    apply_operation(working_map, operation)
  end
end
