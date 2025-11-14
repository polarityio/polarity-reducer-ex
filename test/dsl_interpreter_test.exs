defmodule DslInterpreterTest do
  use ExUnit.Case
  doctest DslInterpreter

  describe "execute/2" do
    test "basic root resolution and output building" do
      data = %{
        "details" => %{"name" => "test", "value" => 42},
        "summary" => "important info"
      }

      dsl = %{
        "root" => %{"path" => "details", "on_null" => "return_empty"},
        "pipeline" => [],
        "output" => %{
          "name" => "$working.name",
          "info" => "$root.summary",
          "full_working" => "$working"
        }
      }

      result = DslInterpreter.execute(data, dsl)

      assert result == %{
        "name" => "test",
        "info" => "important info",
        "full_working" => %{"name" => "test", "value" => 42}
      }
    end

    test "drop operation removes specified paths" do
      data = %{
        "details" => %{
          "keep_me" => "important",
          "remove_me" => "not needed",
          "nested" => %{"also_remove" => "gone", "keep_nested" => "stay"}
        }
      }

      dsl = %{
        "root" => %{"path" => "details"},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["remove_me", "nested.also_remove"]}
        ],
        "output" => %{"result" => "$working"}
      }

      result = DslInterpreter.execute(data, dsl)

      expected_working = %{
        "keep_me" => "important",
        "nested" => %{"keep_nested" => "stay"}
      }

      assert result == %{"result" => expected_working}
    end

    test "project operation creates new structure" do
      data = %{
        "source" => %{
          "users" => [
            %{"id" => 1, "username" => "alice", "email" => "alice@example.com"},
            %{"id" => 2, "username" => "bob", "email" => "bob@example.com"}
          ]
        }
      }

      dsl = %{
        "root" => %{"path" => "source"},
        "pipeline" => [
          %{
            "op" => "project",
            "path" => "users",
            "mapping" => %{
              "user_list" => "users",
              "count" => "users"
            }
          }
        ],
        "output" => %{"result" => "$working"}
      }

      result = DslInterpreter.execute(data, dsl)
      assert Map.has_key?(result["result"], "users")
    end

    test "list_to_map operation converts list to map" do
      data = %{
        "config" => %{
          "settings" => [
            %{"key" => "theme", "value" => "dark"},
            %{"key" => "language", "value" => "en"},
            %{"key" => "notifications", "value" => true}
          ]
        }
      }

      dsl = %{
        "root" => %{"path" => "config"},
        "pipeline" => [
          %{
            "op" => "list_to_map",
            "path" => "settings",
            "key_from" => "key",
            "value_from" => "value"
          }
        ],
        "output" => %{"settings" => "$working.settings"}
      }

      result = DslInterpreter.execute(data, dsl)

      expected_settings = %{
        "theme" => "dark",
        "language" => "en",
        "notifications" => true
      }

      assert result == %{"settings" => expected_settings}
    end

    test "wildcard path handling with arrays" do
      data = %{
        "events" => [
          %{"network" => %{"ip" => "1.1.1.1"}, "keep" => "yes"},
          %{"network" => %{"ip" => "2.2.2.2"}, "keep" => "yes"}
        ]
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{"op" => "drop", "paths" => ["events[].network"]}
        ],
        "output" => %{"events" => "$working.events"}
      }

      result = DslInterpreter.execute(data, dsl)

      expected_events = [
        %{"keep" => "yes"},
        %{"keep" => "yes"}
      ]

      assert result == %{"events" => expected_events}
    end

    test "truncate_list operation with special variables" do
      data = %{
        "items" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      }

      dsl = %{
        "root" => %{"path" => ""},
        "pipeline" => [
          %{
            "op" => "truncate_list",
            "path" => "items",
            "max_size" => 3,
            "shape" => %{
              "total_count" => "$length",
              "first_three" => "$slice(0, 3)",
              "sample" => "truncated"
            }
          }
        ],
        "output" => %{"result" => "$working.items"}
      }

      result = DslInterpreter.execute(data, dsl)

      assert result["result"]["total_count"] == 10
      assert result["result"]["first_three"] == [1, 2, 3]
      assert result["result"]["sample"] == "truncated"
    end

    test "prune operation removes empty values" do
      data = %{
        "messy" => %{
          "keep" => "value",
          "empty_string" => "",
          "nil_value" => nil,
          "empty_map" => %{},
          "empty_list" => [],
          "nested" => %{
            "keep_nested" => "also keep",
            "remove_nested" => ""
          }
        }
      }

      dsl = %{
        "root" => %{"path" => "messy"},
        "pipeline" => [
          %{"op" => "prune", "strategy" => "empty_values"}
        ],
        "output" => %{"cleaned" => "$working"}
      }

      result = DslInterpreter.execute(data, dsl)

      expected = %{
        "cleaned" => %{
          "keep" => "value",
          "nested" => %{
            "keep_nested" => "also keep"
          }
        }
      }

      assert result == expected
    end

    test "on_null return_original behavior" do
      data = %{"summary" => "keep this"}

      dsl = %{
        "root" => %{"path" => "nonexistent", "on_null" => "return_original"},
        "pipeline" => [],
        "output" => %{"summary" => "$working.summary"}
      }

      result = DslInterpreter.execute(data, dsl)
      assert result == %{"summary" => "keep this"}
    end
  end
end
