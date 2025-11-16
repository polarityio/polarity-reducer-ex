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
end
