#!/usr/bin/env elixir

# Comprehensive DSL Pipeline Testing SDK Showcase
# Run with: mix run sdk_showcase.exs

defmodule SdkShowcase do
  def run do
    IO.puts("🧪 DSL Pipeline Testing SDK - Complete Showcase")
    IO.puts("=" |> String.duplicate(60))

    showcase_basic_testing()
    showcase_advanced_features()
    showcase_performance_analysis()
    showcase_debugging_workflow()
    showcase_integration_patterns()

    IO.puts("\n🎉 SDK Showcase Complete!")
    IO.puts("The DSL Pipeline Testing SDK provides comprehensive tools for:")
    IO.puts("  ✅ Step-by-step pipeline execution and visualization")
    IO.puts("  ✅ Performance profiling and bottleneck identification")
    IO.puts("  ✅ Pipeline complexity analysis and optimization suggestions")
    IO.puts("  ✅ File-based testing with JSON data and configurations")
    IO.puts("  ✅ Error handling and debugging support")
    IO.puts("  ✅ Integration with development and testing workflows")
  end

  defp showcase_basic_testing do
    IO.puts("\n📊 1. Basic Pipeline Testing")
    IO.puts("-" |> String.duplicate(40))

    # Simple data transformation
    data = %{
      "orders" => [
        %{"id" => 1, "customer" => "Alice", "amount" => 100.0, "status" => "completed"},
        %{"id" => 2, "customer" => "Bob", "amount" => 50.0, "status" => "pending"},
        %{"id" => 3, "customer" => "Charlie", "amount" => 200.0, "status" => "completed"}
      ]
    }

    pipeline = %{
      "root" => %{"path" => ""},
      "pipeline" => [
        # Filter out pending orders by dropping them (simulation)
        %{"op" => "project_and_replace", "projection" => %{"completed_orders" => "orders"}},
        # Remove order IDs for privacy
        %{"op" => "drop", "paths" => ["completed_orders[].id"]}
      ],
      "output" => %{
        "processed_orders" => "$working.completed_orders",
        "order_count" => "$working.completed_orders"
      }
    }

    result = DslPipelineTester.test_pipeline_direct(data, pipeline, verbose: false)

    if result.success do
      IO.puts("✅ Order processing pipeline executed successfully")
      IO.puts("   • Steps executed: #{result.summary.total_steps}")
      IO.puts("   • Execution time: #{result.total_execution_time_ms}ms")
      IO.puts("   • Data size change: #{result.summary.data_size_reduction}")
    else
      IO.puts("❌ Pipeline failed: #{result.error}")
    end
  end

  defp showcase_advanced_features do
    IO.puts("\n🎯 2. Advanced Pipeline Features")
    IO.puts("-" |> String.duplicate(40))

    # Complex data with nested structures
    data = %{
      "survey_responses" => [
        %{
          "respondent_id" => "resp_001",
          "demographics" => %{"age" => 25, "location" => "US"},
          "answers" => [
            %{"question_id" => "q1", "answer" => "satisfied"},
            %{"question_id" => "q2", "answer" => "yes"},
            %{"question_id" => "q3", "answer" => "7"}
          ]
        },
        %{
          "respondent_id" => "resp_002",
          "demographics" => %{"age" => 35, "location" => "UK"},
          "answers" => [
            %{"question_id" => "q1", "answer" => "very_satisfied"},
            %{"question_id" => "q2", "answer" => "no"},
            %{"question_id" => "q3", "answer" => "9"}
          ]
        }
      ]
    }

    advanced_pipeline = %{
      "root" => %{"path" => ""},
      "pipeline" => [
        # Convert answers to maps for easier access
        %{
          "op" => "list_to_map",
          "path" => "survey_responses[].answers",
          "key_from" => "question_id",
          "value_from" => "answer"
        },
        # Remove respondent IDs for anonymization
        %{"op" => "drop", "paths" => ["survey_responses[].respondent_id"]},
        # Aggregate demographic data
        %{
          "op" => "aggregate_list",
          "path" => "survey_responses",
          "shape" => %{
            "min_age" => "$min(demographics.age)",
            "max_age" => "$max(demographics.age)",
            "total_responses" => "$length"
          }
        }
      ],
      "output" => %{
        "anonymized_data" => "$working.survey_responses"
      }
    }

    result = DslPipelineTester.test_pipeline_direct(data, advanced_pipeline, verbose: false)

    if result.success do
      IO.puts("✅ Survey data anonymization pipeline completed")

      # Show specific transformation details
      steps_with_significant_changes = result.steps
        |> Enum.filter(fn step ->
          step.step > 0 and step.step < length(result.steps) and
          step.execution_time_ms > 0
        end)

      if length(steps_with_significant_changes) > 0 do
        slowest_step = Enum.max_by(steps_with_significant_changes, &(&1.execution_time_ms))
        IO.puts("   • Slowest operation: #{slowest_step.operation} (#{slowest_step.execution_time_ms}ms)")
      end
    end
  end

  defp showcase_performance_analysis do
    IO.puts("\n⚡ 3. Performance Analysis & Optimization")
    IO.puts("-" |> String.duplicate(40))

    # Create a complex pipeline for analysis
    complex_pipeline = %{
      "root" => %{"path" => "data.events"},
      "pipeline" => [
        %{"op" => "drop", "paths" => ["[].debug_info", "[].internal_metadata"]},
        %{"op" => "list_to_map", "path" => "[].attributes", "key_from" => "key", "value_from" => "value"},
        %{"op" => "project", "path" => "", "mapping" => %{"clean_events" => "events"}},
        %{"op" => "hoist_map_values", "path" => "clean_events[].config", "child_key" => "settings"},
        %{"op" => "truncate_list", "path" => "clean_events", "max_size" => 1000, "shape" => %{"total" => "$length"}},
        %{"op" => "aggregate_list", "path" => "clean_events", "shape" => %{"max_score" => "$max(score)"}},
        %{"op" => "prune", "strategy" => "empty_values"},
        %{"op" => "list_to_dynamic_map", "path" => "clean_events[].tags", "key_from" => "category", "value_from" => "tag"},
        %{"op" => "promote_list_to_keys", "path" => "clean_events[].metadata", "child_list" => "flags", "key_from" => "name", "value_from" => "enabled"}
      ],
      "output" => %{"processed_events" => "$working.clean_events"}
    }

    IO.puts("Analyzing complex pipeline with #{length(complex_pipeline["pipeline"])} operations...")
    analysis = DslPipelineTester.analyze_pipeline(complex_pipeline)

    IO.puts("📈 Analysis Results:")
    IO.puts("   • Complexity Score: #{analysis.complexity_score}")
    IO.puts("   • Total Operations: #{analysis.total_operations}")

    if length(analysis.potential_issues) > 0 do
      IO.puts("   • Issues Found: #{length(analysis.potential_issues)}")
    end

    if length(analysis.recommendations) > 0 do
      IO.puts("   • Optimization Suggestions: #{length(analysis.recommendations)}")
    end
  end

  defp showcase_debugging_workflow do
    IO.puts("\n🔍 4. Debugging Workflow Demonstration")
    IO.puts("-" |> String.duplicate(40))

    # Example of debugging a problematic pipeline
    problematic_data = %{
      "items" => [
        %{"name" => "item1", "tags" => nil},  # This might cause issues
        %{"name" => "item2", "tags" => []},   # Empty array
        %{"name" => "item3", "tags" => [%{"key" => "color", "value" => "red"}]}
      ]
    }

    debugging_pipeline = %{
      "root" => %{"path" => ""},
      "pipeline" => [
        # This operation might not work as expected with nil/empty tags
        %{
          "op" => "list_to_map",
          "path" => "items[].tags",
          "key_from" => "key",
          "value_from" => "value"
        },
        # Clean up empty values
        %{"op" => "prune", "strategy" => "empty_values"}
      ],
      "output" => %{"clean_items" => "$working.items"}
    }

    IO.puts("Running pipeline with potentially problematic data...")
    result = DslPipelineTester.test_pipeline_direct(problematic_data, debugging_pipeline, verbose: false)

    if result.success do
      IO.puts("✅ Pipeline handled edge cases gracefully")

      # Examine specific steps for debugging insights
      list_to_map_step = result.steps |> Enum.find(&(&1.operation == "list_to_map"))
      if list_to_map_step do
        IO.puts("   • list_to_map operation completed without errors")
        IO.puts("   • Input had #{map_size(list_to_map_step.input)} keys")
        IO.puts("   • Output has #{map_size(list_to_map_step.output)} keys")
      end

      prune_step = result.steps |> Enum.find(&(&1.operation == "prune"))
      if prune_step do
        IO.puts("   • prune operation cleaned up empty values successfully")
      end
    else
      IO.puts("❌ Pipeline encountered issues: #{result.error}")
      IO.puts("   This is valuable debugging information!")
    end
  end

  defp showcase_integration_patterns do
    IO.puts("\n🔧 5. Integration Patterns")
    IO.puts("-" |> String.duplicate(40))

    # Demonstrate how to use the SDK in different scenarios

    # Pattern 1: Validation in development
    IO.puts("Pattern 1: Development Validation")
    test_data = %{"events" => [%{"id" => 1, "data" => "test"}]}
    test_pipeline = %{
      "root" => %{"path" => ""},
      "pipeline" => [%{"op" => "drop", "paths" => ["events[].id"]}],
      "output" => %{"result" => "$working"}
    }

    result = DslPipelineTester.test_pipeline_direct(test_data, test_pipeline, verbose: false)

    # Quick validation check
    validation_passed = result.success &&
                       result.total_execution_time_ms < 100 &&
                       result.summary.operations_executed == 1

    IO.puts("   ✅ Development validation: #{if validation_passed, do: "PASSED", else: "FAILED"}")

    # Pattern 2: Performance benchmarking
    IO.puts("Pattern 2: Performance Benchmarking")
    large_dataset = %{"items" => Enum.to_list(1..1000)}
    perf_pipeline = %{
      "root" => %{"path" => ""},
      "pipeline" => [
        %{"op" => "truncate_list", "path" => "items", "max_size" => 100, "shape" => %{"count" => "$length"}}
      ],
      "output" => %{"result" => "$working"}
    }

    perf_result = DslPipelineTester.test_pipeline_direct(large_dataset, perf_pipeline, verbose: false)
    performance_acceptable = perf_result.success && perf_result.total_execution_time_ms < 50

    IO.puts("   ✅ Performance benchmark: #{if performance_acceptable, do: "ACCEPTABLE", else: "NEEDS OPTIMIZATION"}")
    IO.puts("      (#{perf_result.total_execution_time_ms}ms for 1000 items)")

    # Pattern 3: Configuration validation
    IO.puts("Pattern 3: Configuration Validation")
    config_to_validate = %{
      "root" => %{"path" => "data"},
      "pipeline" => [
        %{"op" => "drop", "paths" => ["field1", "field2"]},
        %{"op" => "project", "path" => "items", "mapping" => %{"new_name" => "old_name"}}
      ],
      "output" => %{"result" => "$working"}
    }

    config_analysis = DslPipelineTester.analyze_pipeline(config_to_validate)
    config_valid = config_analysis.total_operations > 0 &&
                   config_analysis.complexity_score < 20 &&
                   length(config_analysis.potential_issues) == 0

    IO.puts("   ✅ Configuration validation: #{if config_valid, do: "VALID", else: "HAS ISSUES"}")

    IO.puts("\n💡 Integration Tips:")
    IO.puts("   • Use verbose: false in automated tests")
    IO.puts("   • Set performance thresholds for CI/CD")
    IO.puts("   • Analyze pipelines before production deployment")
    IO.puts("   • Use step-by-step debugging for complex transformations")
  end
end

# Run the comprehensive showcase
SdkShowcase.run()
