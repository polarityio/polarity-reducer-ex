defmodule PolarityReducerEx do
  @moduledoc """
  Documentation for `PolarityReducerEx`.
  """

  @callback execute(data :: map(), dsl_config :: map()) :: any()
  @callback execute_with_steps(data :: map(), dsl_config :: map()) :: map()

end
