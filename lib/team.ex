defmodule BigDataBaller.Team do
  alias BigDataBaller.Util

  @measure_types %{
    four_factors: "Four Factors",
    advanced: "Advanced",
    scoring: "Scoring",
    opponent: "Opponent",
    defense: "Defense"
  }

  def general(start_year \\ 1996, end_year \\ 2018) do
    Enum.each(start_year..end_year, fn year -> 
      Enum.each(Map.keys(@measure_types), fn measure_type -> 
        general_stats_by_year({measure_type, year})
      end)
    end)
  end

  def general_stats_by_year({atom, year}) do
    with season_suffix <- Util.season_year_suffix(year),
         measure_type <- @measure_types[atom],
         s3_path <- "team/#{atom}/#{year}.json",
         {:ok, result} <- Nba.Stats.team_stats(%{"MeasureType" => measure_type, "Season" => "#{year}-#{season_suffix}"}) do
      
      Process.sleep(999)

      Poison.encode!(result)
      |> Util.write_to_s3(s3_path)
    else
      {:error, message} -> IO.puts message
    end
  end
end