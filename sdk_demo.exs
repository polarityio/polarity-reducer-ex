#!/usr/bin/env elixir

# Pipeline Testing SDK Demo
# Run with: mix run sdk_demo.exs

defmodule PipelineSdkDemo do
  def run do
    IO.puts("🧪 DSL Pipeline Testing SDK Demo")
    IO.puts("=" |> String.duplicate(60))

    demo_file_based_testing()
    IO.puts("\n" <> "=" |> String.duplicate(60))

    demo_direct_testing()
    IO.puts("\n" <> "=" |> String.duplicate(60))

    demo_pipeline_analysis()
    IO.puts("\n" <> "=" |> String.duplicate(60))

    demo_error_handling()

    IO.puts("\n✅ All SDK demos completed!")
  end

  defp demo_file_based_testing do
    IO.puts("\n📁 Demo 1: File-based Pipeline Testing")
    IO.puts("Testing with security events data and pipeline...")

    # Test the security events pipeline
    case DslPipelineTester.test_pipeline(
      "examples/security_events.json",
      "examples/security_pipeline.json"
    ) do
      %{success: true} = result ->
        IO.puts("🎉 Security events pipeline test completed successfully!")
        IO.puts("Final result has #{map_size(result.final_result)} top-level keys")

      %{success: false, error: error} ->
        IO.puts("❌ Security events test failed: #{error}")

      {:error, reason} ->
        IO.puts("❌ Could not run test: #{reason}")
    end
  end

  defp demo_direct_testing do
    IO.puts("\n🎯 Demo 2: Direct Pipeline Testing")
    IO.puts("Testing with in-memory data...")

    # Simple transformation example
    data = %{
      "products" => [
        %{"name" => "Laptop", "price" => 999.99, "category" => "Electronics"},
        %{"name" => "Book", "price" => 19.99, "category" => "Books"},
        %{"name" => "Chair", "price" => 199.99, "category" => "Furniture"}
      ]
    }

    pipeline_config = %{
      "root" => %{"path" => ""},
      "pipeline" => [
        %{
          "op" => "project_and_replace",
          "projection" => %{
            "product_catalog" => "products",
            "total_products" => "products"
          }
        },
        %{
          "op" => "aggregate_list",
          "path" => "product_catalog",
          "shape" => %{
            "max_price" => "$max(price)",
            "min_price" => "$min(price)",
            "product_count" => "$length"
          }
        }
      ],
      "output" => %{
        "catalog_summary" => "$working.product_catalog"
      }
    }

    result = DslPipelineTester.test_pipeline_direct(data, pipeline_config)

    if result.success do
      IO.puts("🎉 Direct pipeline test completed successfully!")
    else
      IO.puts("❌ Direct pipeline test failed: #{result.error}")
    end
  end

  defp demo_pipeline_analysis do
    IO.puts("\n🔍 Demo 3: Pipeline Analysis")
    IO.puts("Analyzing a complex pipeline configuration...")

    complex_pipeline = %{
      "root" => %{"path" => "data.events"},
      "pipeline" => [
        %{"op" => "drop", "paths" => ["debug", "internal"]},
        %{"op" => "list_to_map", "path" => "items[].config", "key_from" => "key", "value_from" => "value"},
        %{"op" => "project", "path" => "items", "mapping" => %{"clean_items" => "items"}},
        %{"op" => "truncate_list", "path" => "clean_items", "max_size" => 100, "shape" => %{"count" => "$length"}},
        %{"op" => "prune", "strategy" => "empty_values"},
        %{"op" => "prune", "strategy" => "empty_values"}, # Intentional duplicate for demo
        %{"op" => "aggregate_list", "path" => "clean_items", "shape" => %{"max_score" => "$max(score)"}}
      ],
      "output" => %{"result" => "$working"}
    }

    DslPipelineTester.analyze_pipeline(complex_pipeline)
  end

  defp demo_error_handling do
    IO.puts("\n⚠️  Demo 4: Error Handling")
    IO.puts("Testing with invalid configurations...")

    # Test with missing file
    IO.puts("\n📄 Testing with non-existent file:")
    case DslPipelineTester.test_pipeline("missing.json", "examples/security_pipeline.json") do
      {:error, reason} -> IO.puts("✅ Correctly caught error: #{reason}")
      _ -> IO.puts("❌ Should have failed with file error")
    end

    # Test with invalid DSL config
    IO.puts("\n📄 Testing with invalid DSL config:")
    invalid_data = %{"test" => "data"}
    invalid_dsl = %{"invalid" => "config"}  # Missing required keys

    result = DslPipelineTester.test_pipeline_direct(invalid_data, invalid_dsl, verbose: false)

    if result.success do
      IO.puts("❌ Should have failed with invalid config")
    else
      IO.puts("✅ Correctly handled invalid configuration")
    end

    # Test with operation that causes error
    IO.puts("\n📄 Testing with problematic operation:")
    error_data = %{"items" => "not_a_list"}
    error_dsl = %{
      "root" => %{"path" => ""},
      "pipeline" => [
        %{"op" => "truncate_list", "path" => "items", "max_size" => 5, "shape" => %{"count" => "$length"}}
      ],
      "output" => %{"result" => "$working"}
    }

    result = DslPipelineTester.test_pipeline_direct(error_data, error_dsl, verbose: false)
    IO.puts("Operation completed (graceful handling): #{result.success}")
  end
end

# Run the demo
PipelineSdkDemo.run()
