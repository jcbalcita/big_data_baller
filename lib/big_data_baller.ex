defmodule BigDataBaller do
  @moduledoc """
  """
  @game_date_format "{M}/{D}/{YYYY}"
  @s3_directory_format "{YYYY}/{0M}/{0D}"
  @s3_bucket_name "nba-box-scores-s3"

  def box_scores(start_date, end_date) do
    with {start_datetime, end_datetime} <- get_datetimes(start_date, end_date) do
      step_through_days(start_datetime, end_datetime)
      IO.puts("Completed fetching box scores for the specified time range")
    else
      :error -> IO.puts("Bad dates...")
    end
  end

  def fetch_box_scores(datetime) do
    case Nba.Stats.scoreboard(%{"gameDate" => Timex.format!(datetime, @game_date_format)})["GameHeader"] do
      nil ->
        IO.puts("you got rate limited, dumbass")

      game_headers ->
        game_headers
        # |> Enum.map(&Task.async(fn -> process_game(&1, datetime) end))
        # |> Enum.map(&Task.await/1)
        |> Enum.each(fn header -> process_game(header, datetime) end)
    end
  end

  defp process_game(header, datetime) do
    season_start_year = header["SEASON"]

    season_end_year =
      (elem(Integer.parse(season_start_year), 0) + 1)
      |> Integer.to_string()
      |> String.slice(-2..-1)

    gid = header["GAME_ID"]
    [_, game_code] = String.split(header["GAMECODE"], "/")
    year_month_day = Timex.format!(datetime, @s3_directory_format)
    s3_path = "#{season_start_year}/#{year_month_day}/#{gid}-#{game_code}.json"

    Process.sleep(1000)

    Nba.Stats.box_score(%{"GameID" => gid, "Season" => "#{season_start_year}-#{season_end_year}"})
    |> Poison.encode!()
    |> write_to_s3(s3_path)
  end

  def write_to_s3(json, path) do
    ExAws.S3.put_object(@s3_bucket_name, path, json)
    |> ExAws.request()
  end

  defp get_datetimes(start_date, end_date) do
    case {Timex.to_datetime(start_date), Timex.to_datetime(end_date)} do
      {{:error, _}, _} -> :error
      {_, {:error, _}} -> :error
      {sdt, edt} -> {sdt, edt}
      _ -> :error
    end
  end

  defp step_through_days(datetime, end_datetime) do
    case Timex.after?(datetime, end_datetime) do
      true ->
        IO.puts("Done stepping through days")

      false ->
        fetch_box_scores(datetime)
        step_through_days(Timex.add(datetime, Timex.Duration.from_days(1)), end_datetime)
    end
  end
end
