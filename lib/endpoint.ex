defmodule BigDataBaller.Endpoint do
  alias Nba.Stats

  defstruct [:name, :type, :date_format, :query_map, :increment, :get_date]

  def get(e) do
    with {:ok, response} <- apply(Stats, e.name, e.query_map), 
         {:ok, date} <- e.get_date.(response) do
      IO.puts response
    end
  end
end