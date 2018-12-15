defmodule BigDataBaller do
  @moduledoc """
  """
  @game_date_format "{M}/{D}/{YYYY}"
  @s3_directory_format "{YYYY}/{0M}/{0D}"
  @s3_bucket_name "nba-box-scores-s3"

  def run({year, month, start_day}, end_day) do
    start_day..end_day
    |> Enum.each(fn day ->
      fetch(Timex.to_datetime({year, month, day}))
    end)
  end

  def fetch(date_time) do
    case Nba.Stats.scoreboard(%{"gameDate" => Timex.format!(date_time, @game_date_format)})["GameHeader"] do
      nil ->
        IO.puts("you got rate limited, dumbass")

      game_headers ->
        game_headers
        # |> Enum.map(&Task.async(fn -> process_game(&1, date_time) end))
        |> Enum.each(fn header -> process_game(header, date_time) end)
        # |> Enum.map(&Task.await/1)
    end
  end

  def date_string(date_time, format) do
    Timex.format!(date_time, format)
  end

  def process_game(header, date_time) do
    season_start_year = header["SEASON"]
    season_end_year =
      elem(Integer.parse(season_start_year), 0) + 1
      |> Integer.to_string()
      |> String.slice(-2..-1)

    gid = header["GAME_ID"]
    [_, game_code] = String.split(header["GAMECODE"], "/")
    year_month_day = Timex.format!(date_time, @s3_directory_format)
    s3_path = "#{season_start_year}/#{year_month_day}/#{gid}-#{game_code}.json"

    Nba.Stats.box_score(%{"GameID" => gid, "Season" => "#{season_start_year}-#{season_end_year}"})
    |> Poison.encode!()
    |> write_to_s3(s3_path)
  end

  def write_to_s3(json, path) do
    ExAws.S3.put_object(@s3_bucket_name, path, json)
    |> ExAws.request()
  end
end
