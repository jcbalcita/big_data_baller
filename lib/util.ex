defmodule BigDataBaller.Util do
  def season_year_suffix(season_start_year) when is_integer(season_start_year) do
    Integer.to_string(season_start_year)
    |> String.slice(-2..-1)
  end

  def season_year_suffix(season_start_year) when is_binary(season_start_year) do
    (elem(Integer.parse(season_start_year), 0) + 1)
    |> season_year_suffix()
  end

  def season_year_from_filepath(filepath) do
    {season_start_year, _} = filepath |> String.split("/") |> Enum.at(1) |> Integer.parse()
    season_end_year = (season_start_year + 1) |> Integer.to_string() |> String.slice(2..3)

    "#{season_start_year}" <> "-" <> season_end_year
  end

  def home_and_away_teams_from_filepath(filepath) do
    String.split(filepath, "/")
    |> List.last()
    |> String.split("-")
    |> List.last()
    |> String.split(".")
    |> List.first()
    |> String.split_at(3)
  end
end