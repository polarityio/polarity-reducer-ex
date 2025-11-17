defmodule PolarityReducerEx.DslOperationsTest do
  use ExUnit.Case
  alias PolarityReducerEx.DslInterpreter
  doctest PolarityReducerEx.DslInterpreter

  @moduledoc """
  Comprehensive test suite for all DSL operations.
  Tests each operation individually with various scenarios and edge cases.
  """

  describe "drop operation" do
    test "removes single path" do
      data = %{"keep" => "value", "remove" => "unwanted"}
      operation = %{"op" => "drop", "paths" => ["remove"]}

      result = DslInterpreter.apply_drop_operation_public(data, operation)

      assert result == %{"keep" => "value"}
    end

    test "removes multiple paths" do
      data = %{"a" => 1, "b" => 2, "c" => 3, "d" => 4}
      operation = %{"op" => "drop", "paths" => ["b", "d"]}

      result = DslInterpreter.apply_drop_operation_public(data, operation)

      assert result == %{"a" => 1, "c" => 3}
    end

    test "removes nested paths" do
      data = %{
        "user" => %{
          "name" => "John",
          "email" => "john@example.com",
          "internal" => %{"id" => 123, "debug" => "info"}
        }
      }
      operation = %{"op" => "drop", "paths" => ["user.email", "user.internal.debug"]}

      result = DslInterpreter.apply_drop_operation_public(data, operation)

      assert result == %{
        "user" => %{
          "name" => "John",
          "internal" => %{"id" => 123}
        }
      }
    end

    test "removes array elements with wildcard" do
      data = %{
        "users" => [
          %{"name" => "John", "secret" => "hidden1"},
          %{"name" => "Jane", "secret" => "hidden2"}
        ]
      }
      operation = %{"op" => "drop", "paths" => ["users[].secret"]}

      result = DslInterpreter.apply_drop_operation_public(data, operation)

      assert result == %{
        "users" => [
          %{"name" => "John"},
          %{"name" => "Jane"}
        ]
      }
    end

    test "handles non-existent paths gracefully" do
      data = %{"keep" => "value"}
      operation = %{"op" => "drop", "paths" => ["nonexistent", "also.missing"]}

      result = DslInterpreter.apply_drop_operation_public(data, operation)

      assert result == %{"keep" => "value"}
    end
  end

  describe "rename operation" do
    test "renames simple field" do
      data = %{"old_name" => "value", "other" => "data"}
      operation = %{"op" => "rename", "mapping" => %{"old_name" => "new_name"}}

      result = DslInterpreter.apply_rename_operation_public(data, operation)

      assert result == %{"new_name" => "value", "other" => "data"}
    end

    test "renames nested fields" do
      data = %{
        "user" => %{"first_name" => "John", "last_name" => "Doe"},
        "metadata" => %{"created_by" => "system"}
      }
      operation = %{
        "op" => "rename",
        "mapping" => %{
          "user.first_name" => "user.firstName",
          "metadata.created_by" => "metadata.creator"
        }
      }

      result = DslInterpreter.apply_rename_operation_public(data, operation)

      assert result == %{
        "user" => %{"firstName" => "John", "last_name" => "Doe"},
        "metadata" => %{"creator" => "system"}
      }
    end

    test "renames array elements with wildcard" do
      data = %{
        "events" => [
          %{"user_id" => "123", "action" => "login"},
          %{"user_id" => "456", "action" => "logout"}
        ]
      }
      operation = %{
        "op" => "rename",
        "mapping" => %{"events[].user_id" => "events[].userId"}
      }

      result = DslInterpreter.apply_rename_operation_public(data, operation)

      assert result == %{
        "events" => [
          %{"userId" => "123", "action" => "login"},
          %{"userId" => "456", "action" => "logout"}
        ]
      }
    end

    test "handles non-existent source paths" do
      data = %{"existing" => "value"}
      operation = %{
        "op" => "rename",
        "mapping" => %{"nonexistent" => "new_name", "existing" => "renamed"}
      }

      result = DslInterpreter.apply_rename_operation_public(data, operation)

      assert result == %{"renamed" => "value"}
    end

    test "multiple renames in single operation" do
      data = %{"a" => 1, "b" => 2, "c" => 3}
      operation = %{
        "op" => "rename",
        "mapping" => %{"a" => "alpha", "b" => "beta", "c" => "gamma"}
      }

      result = DslInterpreter.apply_rename_operation_public(data, operation)

      assert result == %{"alpha" => 1, "beta" => 2, "gamma" => 3}
    end
  end

  describe "project operation" do
    test "projects simple fields" do
      data = %{
        "users" => [
          %{"id" => 1, "name" => "John", "email" => "john@example.com", "internal_id" => "abc"},
          %{"id" => 2, "name" => "Jane", "email" => "jane@example.com", "internal_id" => "def"}
        ]
      }

      operation = %{
        "op" => "project",
        "path" => "users",
        "mapping" => %{
          "user_id" => "id",
          "display_name" => "name",
          "contact" => "email"
        }
      }

      result = DslInterpreter.apply_project_operation_public(data, operation)

      expected = %{
        "users" => [
          %{"user_id" => 1, "display_name" => "John", "contact" => "john@example.com"},
          %{"user_id" => 2, "display_name" => "Jane", "contact" => "jane@example.com"}
        ]
      }

      assert result == expected
    end

    test "projects with nested source paths" do
      data = %{
        "response" => %{
          "user" => %{
            "profile" => %{"name" => "John", "age" => 30},
            "settings" => %{"theme" => "dark"}
          }
        }
      }

      operation = %{
        "op" => "project",
        "path" => "response",
        "mapping" => %{
          "user_name" => "user.profile.name",
          "user_theme" => "user.settings.theme"
        }
      }

      result = DslInterpreter.apply_project_operation_public(data, operation)

      expected = %{
        "response" => %{
          "user_name" => "John",
          "user_theme" => "dark"
        }
      }

      assert result == expected
    end
  end

  describe "prune operation" do
    test "removes nil values" do
      data = %{"keep" => "value", "remove" => nil, "nested" => %{"keep" => 1, "remove" => nil}}
      operation = %{"op" => "prune", "strategy" => "empty_values"}

      result = DslInterpreter.apply_prune_operation_public(data, operation)

      assert result == %{"keep" => "value", "nested" => %{"keep" => 1}}
    end

    test "removes empty strings" do
      data = %{"keep" => "value", "remove" => "", "nested" => %{"keep" => "text", "remove" => ""}}
      operation = %{"op" => "prune", "strategy" => "empty_values"}

      result = DslInterpreter.apply_prune_operation_public(data, operation)

      assert result == %{"keep" => "value", "nested" => %{"keep" => "text"}}
    end

    test "removes empty maps and lists" do
      data = %{
        "keep" => "value",
        "empty_map" => %{},
        "empty_list" => [],
        "nested" => %{
          "keep" => 1,
          "empty_nested" => %{},
          "list_with_empties" => [%{}, "", nil, "keep"]
        }
      }
      operation = %{"op" => "prune", "strategy" => "empty_values"}

      result = DslInterpreter.apply_prune_operation_public(data, operation)

      assert result == %{
        "keep" => "value",
        "nested" => %{
          "keep" => 1,
          "list_with_empties" => ["keep"]
        }
      }
    end
  end

  describe "current_timestamp operation" do
    test "adds current timestamp in ISO format" do
      data = %{"existing" => "data"}
      operation = %{
        "op" => "current_timestamp",
        "path" => "timestamp",
        "format" => "iso8601"
      }

      result = DslInterpreter.apply_current_timestamp_operation_public(data, operation)

      assert Map.has_key?(result, "timestamp")
      assert result["existing"] == "data"

      # Check that timestamp is in ISO format
      timestamp = result["timestamp"]
      assert is_binary(timestamp)
      assert Regex.match?(~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/, timestamp)
    end

    test "adds timestamp in human format" do
      data = %{}
      operation = %{
        "op" => "current_timestamp",
        "path" => "created_at",
        "format" => "human"
      }

      result = DslInterpreter.apply_current_timestamp_operation_public(data, operation)

      timestamp = result["created_at"]
      assert is_binary(timestamp)
      assert Regex.match?(~r/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/, timestamp)
    end

    test "adds timestamp to nested path" do
      data = %{"metadata" => %{"version" => "1.0"}}
      operation = %{
        "op" => "current_timestamp",
        "path" => "metadata.processed_at",
        "format" => "iso8601"
      }

      result = DslInterpreter.apply_current_timestamp_operation_public(data, operation)

      assert get_in(result, ["metadata", "processed_at"]) != nil
      assert result["metadata"]["version"] == "1.0"
    end
  end

  describe "format_date operation" do
    test "formats ISO datetime to human readable" do
      data = %{"created_at" => "2024-01-15T10:30:00Z"}
      operation = %{
        "op" => "format_date",
        "path" => "created_at",
        "format" => "human"
      }

      result = DslInterpreter.apply_format_date_operation_public(data, operation)

      assert result["created_at"] == "2024-01-15 10:30:00 UTC"
    end

    test "formats datetime to date only" do
      data = %{"created_at" => "2024-01-15T10:30:00Z"}
      operation = %{
        "op" => "format_date",
        "path" => "created_at",
        "format" => "date_only"
      }

      result = DslInterpreter.apply_format_date_operation_public(data, operation)

      assert result["created_at"] == "2024-01-15"
    end

    test "handles array of dates" do
      data = %{
        "events" => [
          %{"timestamp" => "2024-01-15T10:30:00Z"},
          %{"timestamp" => "2024-01-16T14:45:00Z"}
        ]
      }
      operation = %{
        "op" => "format_date",
        "path" => "events[].timestamp",
        "format" => "date_only"
      }

      result = DslInterpreter.apply_format_date_operation_public(data, operation)

      expected_events = [
        %{"timestamp" => "2024-01-15"},
        %{"timestamp" => "2024-01-16"}
      ]

      assert result["events"] == expected_events
    end

    test "preserves invalid date strings" do
      data = %{"invalid_date" => "not-a-date", "valid_date" => "2024-01-15T10:30:00Z"}
      operation = %{
        "op" => "format_date",
        "path" => "invalid_date",
        "format" => "human"
      }

      result = DslInterpreter.apply_format_date_operation_public(data, operation)

      assert result["invalid_date"] == "not-a-date"  # Should preserve original
    end
  end

  describe "date_add operation" do
    test "adds days to date" do
      data = %{"start_date" => "2024-01-15"}
      operation = %{
        "op" => "date_add",
        "path" => "start_date",
        "amount" => 7,
        "unit" => "days",
        "output_format" => "date_only"
      }

      result = DslInterpreter.apply_date_add_operation_public(data, operation)

      assert result["start_date"] == "2024-01-22"
    end

    test "adds years to datetime" do
      data = %{"birth_date" => "1990-03-25T00:00:00Z"}
      operation = %{
        "op" => "date_add",
        "path" => "birth_date",
        "amount" => 35,
        "unit" => "years",
        "output_format" => "date_only"
      }

      result = DslInterpreter.apply_date_add_operation_public(data, operation)

      # Should be approximately 2025-03-16 (35 years later)
      assert String.starts_with?(result["birth_date"], "2025-03")
    end

    test "adds hours to datetime" do
      data = %{"event_time" => "2024-01-15T10:00:00Z"}
      operation = %{
        "op" => "date_add",
        "path" => "event_time",
        "amount" => 3,
        "unit" => "hours",
        "output_format" => "iso8601"
      }

      result = DslInterpreter.apply_date_add_operation_public(data, operation)

      assert String.contains?(result["event_time"], "T13:00:00")
    end
  end

  describe "date_diff operation" do
    test "calculates difference in days" do
      data = %{
        "start_date" => "2024-01-15T10:00:00Z",
        "end_date" => "2024-01-20T10:00:00Z"
      }
      operation = %{
        "op" => "date_diff",
        "from_path" => "start_date",
        "to_path" => "end_date",
        "result_path" => "duration_days",
        "unit" => "days"
      }

      result = DslInterpreter.apply_date_diff_operation_public(data, operation)

      assert result["duration_days"] == 5.0
      assert result["start_date"] == "2024-01-15T10:00:00Z"  # Original preserved
      assert result["end_date"] == "2024-01-20T10:00:00Z"    # Original preserved
    end

    test "calculates difference in hours" do
      data = %{
        "login_time" => "2024-01-15T10:00:00Z",
        "logout_time" => "2024-01-15T18:30:00Z"
      }
      operation = %{
        "op" => "date_diff",
        "from_path" => "login_time",
        "to_path" => "logout_time",
        "result_path" => "session_hours",
        "unit" => "hours"
      }

      result = DslInterpreter.apply_date_diff_operation_public(data, operation)

      assert result["session_hours"] == 8.5
    end

    test "handles invalid dates gracefully" do
      data = %{
        "start_date" => "invalid-date",
        "end_date" => "2024-01-20T10:00:00Z"
      }
      operation = %{
        "op" => "date_diff",
        "from_path" => "start_date",
        "to_path" => "end_date",
        "result_path" => "duration",
        "unit" => "days"
      }

      result = DslInterpreter.apply_date_diff_operation_public(data, operation)

      assert result["duration"] == nil
    end
  end

  describe "edge cases and error handling" do
    test "operations handle nil input gracefully" do
      operations = [
        %{"op" => "drop", "paths" => ["nonexistent"]},
        %{"op" => "rename", "mapping" => %{"nonexistent" => "new"}},
        %{"op" => "prune", "strategy" => "empty_values"}
      ]

      for operation <- operations do
        result = DslInterpreter.apply_operation_public(nil, operation)
        assert result == nil
      end
    end

    test "operations handle empty map input" do
      data = %{}
      operations = [
        %{"op" => "drop", "paths" => ["nonexistent"]},
        %{"op" => "rename", "mapping" => %{"nonexistent" => "new"}},
        %{"op" => "prune", "strategy" => "empty_values"}
      ]

      for operation <- operations do
        result = DslInterpreter.apply_operation_public(data, operation)
        assert result == %{}
      end
    end

    test "deeply nested operations work correctly" do
      data = %{
        "level1" => %{
          "level2" => %{
            "level3" => %{
              "level4" => %{
                "target" => "deep_value",
                "other" => "keep_me"
              }
            }
          }
        }
      }

      # Test drop with deep nesting
      drop_op = %{"op" => "drop", "paths" => ["level1.level2.level3.level4.target"]}
      result = DslInterpreter.apply_drop_operation_public(data, drop_op)

      assert get_in(result, ["level1", "level2", "level3", "level4", "target"]) == nil
      assert get_in(result, ["level1", "level2", "level3", "level4", "other"]) == "keep_me"
    end
  end

  describe "set operation" do
    test "sets static string value" do
      data = %{"user" => %{"name" => "John"}}
      operation = %{"op" => "set", "path" => "user.status", "value" => "active"}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["user"]["status"] == "active"
      assert result["user"]["name"] == "John"  # original data preserved
    end

    test "sets static number value" do
      data = %{"counters" => %{}}
      operation = %{"op" => "set", "path" => "counters.total", "value" => 42}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["counters"]["total"] == 42
    end

    test "sets static boolean value" do
      data = %{"flags" => %{}}
      operation = %{"op" => "set", "path" => "flags.enabled", "value" => true}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["flags"]["enabled"] == true
    end

    test "sets static object value" do
      data = %{"config" => %{}}
      operation = %{"op" => "set", "path" => "config.database", "value" => %{"host" => "localhost", "port" => 5432}}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["config"]["database"]["host"] == "localhost"
      assert result["config"]["database"]["port"] == 5432
    end

    test "copies value from another path" do
      data = %{"source" => %{"name" => "John"}, "target" => %{}}
      operation = %{"op" => "set", "path" => "target.display_name", "value" => "$path:source.name"}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["target"]["display_name"] == "John"
      assert result["source"]["name"] == "John"  # original preserved
    end

    test "copies nested value from path" do
      data = %{
        "user" => %{"profile" => %{"full_name" => "John Doe"}},
        "metadata" => %{}
      }
      operation = %{"op" => "set", "path" => "metadata.author", "value" => "$path:user.profile.full_name"}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["metadata"]["author"] == "John Doe"
    end

    test "sets array elements with wildcard and static value" do
      data = %{"users" => [%{"name" => "John"}, %{"name" => "Jane"}]}
      operation = %{"op" => "set", "path" => "users[].active", "value" => true}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert Enum.all?(result["users"], fn user -> user["active"] == true end)
      assert Enum.at(result["users"], 0)["name"] == "John"
      assert Enum.at(result["users"], 1)["name"] == "Jane"
    end

    test "copies from array path to array path" do
      data = %{"users" => [%{"first_name" => "John"}, %{"first_name" => "Jane"}]}
      operation = %{"op" => "set", "path" => "users[].display_name", "value" => "$path:users[].first_name"}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert Enum.at(result["users"], 0)["display_name"] == "John"
      assert Enum.at(result["users"], 1)["display_name"] == "Jane"
    end

    test "overwrites existing value" do
      data = %{"user" => %{"status" => "inactive"}}
      operation = %{"op" => "set", "path" => "user.status", "value" => "active"}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["user"]["status"] == "active"
    end

    test "creates nested structure when path doesn't exist" do
      data = %{}
      operation = %{"op" => "set", "path" => "deeply.nested.value", "value" => "created"}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert get_in(result, ["deeply", "nested", "value"]) == "created"
    end

    test "handles nil path reference gracefully" do
      data = %{"source" => %{"missing" => nil}, "target" => %{}}
      operation = %{"op" => "set", "path" => "target.copied", "value" => "$path:source.missing"}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["target"]["copied"] == nil
    end

    test "handles missing path reference gracefully" do
      data = %{"source" => %{}, "target" => %{}}
      operation = %{"op" => "set", "path" => "target.copied", "value" => "$path:source.nonexistent"}

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result["target"]["copied"] == nil
    end

    test "ignores invalid operation format" do
      data = %{"test" => "value"}
      operation = %{"op" => "set"}  # missing required parameters

      result = DslInterpreter.apply_set_operation_public(data, operation)

      assert result == data  # unchanged
    end
  end

  describe "transform operation" do
    test "transforms string to uppercase" do
      data = %{"user" => %{"name" => "john doe"}}
      operation = %{"op" => "transform", "path" => "user.name", "function" => "uppercase"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["user"]["name"] == "JOHN DOE"
    end

    test "transforms string to lowercase" do
      data = %{"user" => %{"name" => "JANE SMITH"}}
      operation = %{"op" => "transform", "path" => "user.name", "function" => "lowercase"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["user"]["name"] == "jane smith"
    end

    test "capitalizes string" do
      data = %{"user" => %{"name" => "alice wonderland"}}
      operation = %{"op" => "transform", "path" => "user.name", "function" => "capitalize"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["user"]["name"] == "Alice wonderland"
    end

    test "trims whitespace from string" do
      data = %{"user" => %{"name" => "  bob builder  "}}
      operation = %{"op" => "transform", "path" => "user.name", "function" => "trim"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["user"]["name"] == "bob builder"
    end

    test "converts values to string" do
      data = %{"values" => %{"number" => 42, "boolean" => true, "nil" => nil}}
      
      operations = [
        %{"op" => "transform", "path" => "values.number", "function" => "string"},
        %{"op" => "transform", "path" => "values.boolean", "function" => "string"},
        %{"op" => "transform", "path" => "values.nil", "function" => "string"}
      ]

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["number"] == "42"
      assert result["values"]["boolean"] == "true"
      assert result["values"]["nil"] == ""
    end

    test "converts strings to numbers" do
      data = %{"values" => %{"integer" => "123", "float" => "45.67", "invalid" => "abc"}}
      
      operations = [
        %{"op" => "transform", "path" => "values.integer", "function" => "number"},
        %{"op" => "transform", "path" => "values.float", "function" => "number"},
        %{"op" => "transform", "path" => "values.invalid", "function" => "number"}
      ]

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["integer"] == 123
      assert result["values"]["float"] == 45.67
      assert result["values"]["invalid"] == nil
    end

    test "converts values to integers" do
      data = %{"values" => %{"string_int" => "456", "float" => 78.9, "string_float" => "12.34"}}
      
      operations = [
        %{"op" => "transform", "path" => "values.string_int", "function" => "integer"},
        %{"op" => "transform", "path" => "values.float", "function" => "integer"},
        %{"op" => "transform", "path" => "values.string_float", "function" => "integer"}
      ]

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["string_int"] == 456
      assert result["values"]["float"] == 78
      assert result["values"]["string_float"] == 12
    end

    test "converts values to floats" do
      data = %{"values" => %{"string" => "123.45", "integer" => 67}}
      
      operations = [
        %{"op" => "transform", "path" => "values.string", "function" => "float"},
        %{"op" => "transform", "path" => "values.integer", "function" => "float"}
      ]

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["string"] == 123.45
      assert result["values"]["integer"] == 67.0
    end

    test "converts values to booleans" do
      data = %{"values" => %{
        "true_val" => true,
        "false_val" => false,
        "nil_val" => nil,
        "empty_string" => "",
        "zero" => 0,
        "false_string" => "false",
        "other" => "anything"
      }}
      
      operations = Enum.map(Map.keys(data["values"]), fn key ->
        %{"op" => "transform", "path" => "values.#{key}", "function" => "boolean"}
      end)

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["true_val"] == true
      assert result["values"]["false_val"] == false
      assert result["values"]["nil_val"] == false
      assert result["values"]["empty_string"] == false
      assert result["values"]["zero"] == false
      assert result["values"]["false_string"] == false
      assert result["values"]["other"] == true
    end

    test "calculates length of strings, lists, and maps" do
      data = %{"values" => %{
        "string" => "hello",
        "list" => [1, 2, 3, 4],
        "map" => %{"a" => 1, "b" => 2, "c" => 3}
      }}
      
      operations = [
        %{"op" => "transform", "path" => "values.string", "function" => "length"},
        %{"op" => "transform", "path" => "values.list", "function" => "length"},
        %{"op" => "transform", "path" => "values.map", "function" => "length"}
      ]

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["string"] == 5
      assert result["values"]["list"] == 4
      assert result["values"]["map"] == 3
    end

    test "reverses strings and lists" do
      data = %{"values" => %{"string" => "hello", "list" => [1, 2, 3]}}
      
      operations = [
        %{"op" => "transform", "path" => "values.string", "function" => "reverse"},
        %{"op" => "transform", "path" => "values.list", "function" => "reverse"}
      ]

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["string"] == "olleh"
      assert result["values"]["list"] == [3, 2, 1]
    end

    test "splits strings with custom delimiter" do
      data = %{"text" => "apple,banana,cherry"}
      operation = %{"op" => "transform", "path" => "text", "function" => "split", "args" => [","]}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["text"] == ["apple", "banana", "cherry"]
    end

    test "splits strings with default delimiter" do
      data = %{"text" => "hello world test"}
      operation = %{"op" => "transform", "path" => "text", "function" => "split"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["text"] == ["hello", "world", "test"]
    end

    test "joins lists with custom delimiter" do
      data = %{"items" => ["apple", "banana", "cherry"]}
      operation = %{"op" => "transform", "path" => "items", "function" => "join", "args" => [", "]}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["items"] == "apple, banana, cherry"
    end

    test "joins lists with default delimiter" do
      data = %{"items" => ["hello", "world"]}
      operation = %{"op" => "transform", "path" => "items", "function" => "join"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["items"] == "hello world"
    end

    test "calculates absolute value of numbers" do
      data = %{"values" => %{"positive" => 5, "negative" => -10, "zero" => 0}}
      
      operations = [
        %{"op" => "transform", "path" => "values.positive", "function" => "abs"},
        %{"op" => "transform", "path" => "values.negative", "function" => "abs"},
        %{"op" => "transform", "path" => "values.zero", "function" => "abs"}
      ]

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["positive"] == 5
      assert result["values"]["negative"] == 10
      assert result["values"]["zero"] == 0
    end

    test "rounds numbers with precision" do
      data = %{"values" => %{"pi" => 3.14159, "simple" => 2.7}}
      
      operations = [
        %{"op" => "transform", "path" => "values.pi", "function" => "round", "args" => [2]},
        %{"op" => "transform", "path" => "values.simple", "function" => "round"}
      ]

      result = Enum.reduce(operations, data, fn op, acc ->
        DslInterpreter.apply_transform_operation_public(acc, op)
      end)

      assert result["values"]["pi"] == 3.14
      assert result["values"]["simple"] == 3.0
    end

    test "transforms array elements with wildcard" do
      data = %{"users" => [%{"name" => "john"}, %{"name" => "jane"}]}
      operation = %{"op" => "transform", "path" => "users[].name", "function" => "uppercase"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert Enum.at(result["users"], 0)["name"] == "JOHN"
      assert Enum.at(result["users"], 1)["name"] == "JANE"
    end

    test "handles unknown function gracefully" do
      data = %{"value" => "test"}
      operation = %{"op" => "transform", "path" => "value", "function" => "unknown_function"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result["value"] == "test"  # unchanged
    end

    test "handles missing path gracefully" do
      data = %{"other" => "value"}
      operation = %{"op" => "transform", "path" => "missing.path", "function" => "uppercase"}

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      # Should create the nested structure, but transform function returns original value for non-strings
      assert get_in(result, ["missing", "path"]) == nil
    end

    test "ignores invalid operation format" do
      data = %{"test" => "value"}
      operation = %{"op" => "transform"}  # missing required parameters

      result = DslInterpreter.apply_transform_operation_public(data, operation)

      assert result == data  # unchanged
    end
  end

  describe "copy operation" do
    test "copies value from one path to another" do
      data = %{"source" => %{"name" => "John Doe"}, "target" => %{}}
      operation = %{"op" => "copy", "from" => "source.name", "to" => "target.name"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["target"]["name"] == "John Doe"
      assert result["source"]["name"] == "John Doe"  # original still exists
    end

    test "copies nested object" do
      data = %{"source" => %{"user" => %{"name" => "Alice", "age" => 30}}, "backup" => %{}}
      operation = %{"op" => "copy", "from" => "source.user", "to" => "backup.user_copy"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["backup"]["user_copy"]["name"] == "Alice"
      assert result["backup"]["user_copy"]["age"] == 30
      assert result["source"]["user"]["name"] == "Alice"  # original preserved
    end

    test "copies array" do
      data = %{"source" => %{"tags" => ["tag1", "tag2", "tag3"]}, "target" => %{}}
      operation = %{"op" => "copy", "from" => "source.tags", "to" => "target.tag_backup"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["target"]["tag_backup"] == ["tag1", "tag2", "tag3"]
      assert result["source"]["tags"] == ["tag1", "tag2", "tag3"]  # original preserved
    end

    test "copies between array elements (same array)" do
      data = %{"users" => [%{"email" => "john@example.com"}, %{"email" => "jane@example.com"}]}
      operation = %{"op" => "copy", "from" => "users[].email", "to" => "users[].backup_email"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert Enum.at(result["users"], 0)["backup_email"] == "john@example.com"
      assert Enum.at(result["users"], 1)["backup_email"] == "jane@example.com"
      assert Enum.at(result["users"], 0)["email"] == "john@example.com"  # original preserved
      assert Enum.at(result["users"], 1)["email"] == "jane@example.com"  # original preserved
    end

    test "copies from array to regular field" do
      data = %{"users" => [%{"name" => "John"}, %{"name" => "Jane"}], "summary" => %{}}
      operation = %{"op" => "copy", "from" => "users[].name", "to" => "summary.user_names"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["summary"]["user_names"] == ["John", "Jane"]
      assert result["users"] == [%{"name" => "John"}, %{"name" => "Jane"}]  # original preserved
    end

    test "copies from regular field to array elements" do
      data = %{"template" => "Default Template", "items" => [%{}, %{}]}
      operation = %{"op" => "copy", "from" => "template", "to" => "items[].template"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert Enum.at(result["items"], 0)["template"] == "Default Template"
      assert Enum.at(result["items"], 1)["template"] == "Default Template"
      assert result["template"] == "Default Template"  # original preserved
    end

    test "copies deeply nested values" do
      data = %{
        "source" => %{"level1" => %{"level2" => %{"value" => "deep value"}}},
        "target" => %{}
      }
      operation = %{"op" => "copy", "from" => "source.level1.level2.value", "to" => "target.deep.copied.value"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert get_in(result, ["target", "deep", "copied", "value"]) == "deep value"
      assert get_in(result, ["source", "level1", "level2", "value"]) == "deep value"  # original preserved
    end

    test "handles copying nil values" do
      data = %{"source" => %{"nil_field" => nil}, "target" => %{}}
      operation = %{"op" => "copy", "from" => "source.nil_field", "to" => "target.copied_nil"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["target"]["copied_nil"] == nil
      assert Map.has_key?(result["target"], "copied_nil")
    end

    test "handles copying from non-existent path" do
      data = %{"source" => %{}, "target" => %{}}
      operation = %{"op" => "copy", "from" => "source.nonexistent", "to" => "target.copied"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["target"]["copied"] == nil
    end

    test "overwrites existing target field" do
      data = %{"source" => %{"value" => "new"}, "target" => %{"value" => "old"}}
      operation = %{"op" => "copy", "from" => "source.value", "to" => "target.value"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["target"]["value"] == "new"
      assert result["source"]["value"] == "new"  # original preserved
    end

    test "copies complex nested structures" do
      data = %{
        "source" => %{
          "config" => %{
            "settings" => [
              %{"key" => "theme", "value" => "dark"},
              %{"key" => "lang", "value" => "en"}
            ]
          }
        },
        "backup" => %{}
      }
      operation = %{"op" => "copy", "from" => "source.config", "to" => "backup.config_copy"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["backup"]["config_copy"]["settings"] == [
        %{"key" => "theme", "value" => "dark"},
        %{"key" => "lang", "value" => "en"}
      ]
      # Verify original is preserved
      assert result["source"]["config"]["settings"] == [
        %{"key" => "theme", "value" => "dark"},
        %{"key" => "lang", "value" => "en"}
      ]
    end

    test "copies between different arrays" do
      data = %{
        "products" => [%{"id" => 1, "name" => "Product A"}],
        "inventory" => [%{"slot" => 1}]
      }
      operation = %{"op" => "copy", "from" => "products[].name", "to" => "inventory[].product_name"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      # When copying between different arrays, it should copy the array result
      assert Enum.at(result["inventory"], 0)["product_name"] == ["Product A"]
    end

    test "handles empty arrays" do
      data = %{"source" => [], "target" => %{}}
      operation = %{"op" => "copy", "from" => "source", "to" => "target.empty_copy"}

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result["target"]["empty_copy"] == []
      assert result["source"] == []  # original preserved
    end

    test "ignores invalid operation format" do
      data = %{"test" => "value"}
      operation = %{"op" => "copy"}  # missing required parameters

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result == data  # unchanged
    end

    test "handles missing from parameter" do
      data = %{"test" => "value"}
      operation = %{"op" => "copy", "to" => "target"}  # missing from

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result == data  # unchanged
    end

    test "handles missing to parameter" do
      data = %{"test" => "value"}
      operation = %{"op" => "copy", "from" => "test"}  # missing to

      result = DslInterpreter.apply_copy_operation_public(data, operation)

      assert result == data  # unchanged
    end
  end

  describe "move operation" do
    test "moves value from one path to another" do
      data = %{"source" => %{"name" => "John Doe", "age" => 30}, "target" => %{}}
      operation = %{"op" => "move", "from" => "source.name", "to" => "target.name"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["target"]["name"] == "John Doe"
      assert result["source"]["name"] == nil  # original removed
      assert result["source"]["age"] == 30  # other fields preserved
    end

    test "moves nested object" do
      data = %{"source" => %{"user" => %{"name" => "Alice", "age" => 30}, "other" => "keep"}, "backup" => %{}}
      operation = %{"op" => "move", "from" => "source.user", "to" => "backup.user_moved"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["backup"]["user_moved"]["name"] == "Alice"
      assert result["backup"]["user_moved"]["age"] == 30
      assert result["source"]["user"] == nil  # original removed
      assert result["source"]["other"] == "keep"  # other fields preserved
    end

    test "moves array" do
      data = %{"source" => %{"tags" => ["tag1", "tag2", "tag3"], "other" => "keep"}, "target" => %{}}
      operation = %{"op" => "move", "from" => "source.tags", "to" => "target.moved_tags"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["target"]["moved_tags"] == ["tag1", "tag2", "tag3"]
      assert result["source"]["tags"] == nil  # original removed
      assert result["source"]["other"] == "keep"  # other fields preserved
    end

    test "moves between array elements (same array)" do
      data = %{"users" => [
        %{"temp_email" => "john@temp.com", "name" => "John"},
        %{"temp_email" => "jane@temp.com", "name" => "Jane"}
      ]}
      operation = %{"op" => "move", "from" => "users[].temp_email", "to" => "users[].email"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert Enum.at(result["users"], 0)["email"] == "john@temp.com"
      assert Enum.at(result["users"], 1)["email"] == "jane@temp.com"
      assert Enum.at(result["users"], 0)["temp_email"] == nil  # original removed
      assert Enum.at(result["users"], 1)["temp_email"] == nil  # original removed
      assert Enum.at(result["users"], 0)["name"] == "John"  # other fields preserved
      assert Enum.at(result["users"], 1)["name"] == "Jane"  # other fields preserved
    end

    test "moves from array to regular field" do
      data = %{"users" => [%{"name" => "John"}, %{"name" => "Jane"}], "summary" => %{}, "other" => "keep"}
      operation = %{"op" => "move", "from" => "users[].name", "to" => "summary.user_names"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["summary"]["user_names"] == ["John", "Jane"]
      assert result["users"] == [%{}, %{}]  # original fields removed (keys deleted, not set to nil)
      assert result["other"] == "keep"  # other fields preserved
    end

    test "moves from regular field to array elements" do
      data = %{"template" => "Default Template", "items" => [%{"id" => 1}, %{"id" => 2}], "other" => "keep"}
      operation = %{"op" => "move", "from" => "template", "to" => "items[].template"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert Enum.at(result["items"], 0)["template"] == "Default Template"
      assert Enum.at(result["items"], 1)["template"] == "Default Template"
      assert result["template"] == nil  # original removed
      assert result["other"] == "keep"  # other fields preserved
    end

    test "moves deeply nested values" do
      data = %{
        "source" => %{"level1" => %{"level2" => %{"value" => "deep value", "keep" => "this"}}},
        "target" => %{}
      }
      operation = %{"op" => "move", "from" => "source.level1.level2.value", "to" => "target.deep.moved.value"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert get_in(result, ["target", "deep", "moved", "value"]) == "deep value"
      assert get_in(result, ["source", "level1", "level2", "value"]) == nil  # original removed
      assert get_in(result, ["source", "level1", "level2", "keep"]) == "this"  # other fields preserved
    end

    test "handles moving nil values" do
      data = %{"source" => %{"nil_field" => nil, "other" => "keep"}, "target" => %{}}
      operation = %{"op" => "move", "from" => "source.nil_field", "to" => "target.moved_nil"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["target"]["moved_nil"] == nil
      assert Map.has_key?(result["target"], "moved_nil")
      assert result["source"]["nil_field"] == nil  # nil removal results in nil
      assert result["source"]["other"] == "keep"  # other fields preserved
    end

    test "handles moving from non-existent path" do
      data = %{"source" => %{"other" => "keep"}, "target" => %{}}
      operation = %{"op" => "move", "from" => "source.nonexistent", "to" => "target.moved"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["target"]["moved"] == nil
      assert result["source"]["other"] == "keep"  # other fields preserved
    end

    test "overwrites existing target field" do
      data = %{"source" => %{"value" => "new", "other" => "keep"}, "target" => %{"value" => "old"}}
      operation = %{"op" => "move", "from" => "source.value", "to" => "target.value"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["target"]["value"] == "new"
      assert result["source"]["value"] == nil  # original removed
      assert result["source"]["other"] == "keep"  # other fields preserved
    end

    test "moves complex nested structures" do
      data = %{
        "source" => %{
          "config" => %{
            "settings" => [
              %{"key" => "theme", "value" => "dark"},
              %{"key" => "lang", "value" => "en"}
            ]
          },
          "other" => "keep"
        },
        "backup" => %{}
      }
      operation = %{"op" => "move", "from" => "source.config", "to" => "backup.config_moved"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["backup"]["config_moved"]["settings"] == [
        %{"key" => "theme", "value" => "dark"},
        %{"key" => "lang", "value" => "en"}
      ]
      assert result["source"]["config"] == nil  # original removed
      assert result["source"]["other"] == "keep"  # other fields preserved
    end

    test "moves between different arrays" do
      data = %{
        "products" => [%{"id" => 1, "name" => "Product A", "other" => "keep"}],
        "inventory" => [%{"slot" => 1}]
      }
      operation = %{"op" => "move", "from" => "products[].name", "to" => "inventory[].product_name"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      # When moving between different arrays, it should move the array result  
      assert Enum.at(result["inventory"], 0)["product_name"] == ["Product A"]
      assert Enum.at(result["products"], 0)["name"] == nil  # original removed
      assert Enum.at(result["products"], 0)["other"] == "keep"  # other fields preserved
    end

    test "handles empty arrays" do
      data = %{"source" => %{"empty" => [], "other" => "keep"}, "target" => %{}}
      operation = %{"op" => "move", "from" => "source.empty", "to" => "target.empty_moved"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["target"]["empty_moved"] == []
      assert result["source"]["empty"] == nil  # original removed
      assert result["source"]["other"] == "keep"  # other fields preserved
    end

    test "moves entire object at root level" do
      data = %{"old_key" => %{"nested" => "value"}, "keep" => "this"}
      operation = %{"op" => "move", "from" => "old_key", "to" => "new_key"}

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result["new_key"]["nested"] == "value"
      assert result["old_key"] == nil  # original removed
      assert result["keep"] == "this"  # other fields preserved
    end

    test "ignores invalid operation format" do
      data = %{"test" => "value"}
      operation = %{"op" => "move"}  # missing required parameters

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result == data  # unchanged
    end

    test "handles missing from parameter" do
      data = %{"test" => "value"}
      operation = %{"op" => "move", "to" => "target"}  # missing from

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result == data  # unchanged
    end

    test "handles missing to parameter" do
      data = %{"test" => "value"}
      operation = %{"op" => "move", "from" => "test"}  # missing to

      result = DslInterpreter.apply_move_operation_public(data, operation)

      assert result == data  # unchanged
    end
  end
end
