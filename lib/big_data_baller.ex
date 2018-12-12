defmodule BigDataBaller do
  @moduledoc """
  """
  @game_date_format "{M}/{D}/{YYYY}"
  @s3_bucket_name "nba-box-scores-s3"

  def run({year, month, start_day}, end_day) do
    start_day..end_day
    |> Enum.each(fn day ->
      fetch(Timex.to_datetime({year, month, day}))
    end)
  end

  def fetch(date_time) do
    case Nba.Stats.scoreboard(%{"gameDate" => date_string(date_time, @game_date_format)})["GameHeader"] do
      nil ->
        IO.puts("you got rate limited, dumbass")

      headers ->
        headers
        |> Enum.map(&Task.async(fn -> handle_header(&1, date_time) end))
        |> Enum.map(&Task.await/1)
    end
  end

  def date_string(date_time, format) do
    Timex.format!(date_time, format)
  end

  def handle_header(header, date_time) do
    season =
      Timex.format!(date_time, "{YYYY}") <>
        "-" <> Timex.format!(Timex.add(date_time, Timex.Duration.from_days(367)), "{YY}")

    season_dir = header["SEASON"]
    gid = header["GAME_ID"]
    [_, game_code] = String.split(header["GAMECODE"], "/")
    year_month_day = Timex.format!(date_time, "{YYYY}/{0M}/{0D}")
    s3_path = "#{season_dir}/#{year_month_day}/#{gid}-#{game_code}.json"

    Nba.Stats.box_score(%{"GameID" => gid, "Season" => season})
    |> Poison.encode!()
    |> write_to_s3(s3_path)
  end

  def write_to_s3(json, path) do
    ExAws.S3.put_object(@s3_bucket_name, path, json)
    |> ExAws.request()
  end
end
