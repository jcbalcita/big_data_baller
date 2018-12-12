defmodule BigDataBaller do
  @moduledoc """
  """
  @game_date_format "{M}/{D}/{YYYY}"
  @s3_bucket_name "nba-box-scores-s3"

  def run do
    Nba.Stats.scoreboard(%{"gameDate" => date_string(yesterday(), @game_date_format)})["GameHeader"]
    |> Enum.each(fn header ->
      gid = header["GAME_ID"]
      [_, game_code] = String.split(header["GAMECODE"], "/")
      year_month_day = Timex.format!(yesterday, "{YYYY}/{0M}/{0D}")
      season = header["SEASON"]
      s3_path = "#{season}/#{year_month_day}/#{gid}-#{game_code}"

      Nba.Stats.box_score(%{"GameID" => gid})
      |> Poison.encode!()
      |> write_to_s3(s3_path)
    end)
  end

  def yesterday do
    Timex.subtract(Timex.now(Timex.Timezone.local), Timex.Duration.from_days(1))
  end

  def date_string(date_time, format) do
    Timex.format!(date_time, format)
  end

  def write_to_s3(json, path) do
    ExAws.S3.put_object(@s3_bucket_name, path, json)
    |> ExAws.request()
  end
end
