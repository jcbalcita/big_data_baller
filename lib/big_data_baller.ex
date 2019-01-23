defmodule BigDataBaller do
  @moduledoc """
  """
  @game_date_format "{M}/{D}/{YYYY}"
  @s3_directory_format "{YYYY}/{0M}/{0D}"
  @s3_bucket_name "nba-box-scores-s3"

  def box_scores(date) do
    box_scores(date, date)
  end

  def box_scores(start_date, end_date) do
    with :ok <- aws_creds?(),
         {start_datetime, end_datetime} <- get_datetimes(start_date, end_date) do

      step_through_days(start_datetime, end_datetime)
    else
      :date_error -> IO.puts("Bad dates...")
      :aws_error -> IO.puts("No AWS creds... put them in the env and try again")
    end
  end

  def fetch_box_scores(datetime) do
    with {:ok, date_string} <- Timex.format(datetime, @game_date_format),
         response <- Nba.Stats.scoreboard(%{"gameDate" => date_string}),
         game_headers <- Map.get(response, "GameHeader") do

      if game_headers,
        do: Enum.each(game_headers, &process_game(&1, datetime)),
        else: IO.puts("Error fetching scoreboard")
    end
  end

  def team_four_factors(start_year \\ 1996, end_year \\ 2019) do
    Enum.each(start_year..end_year, fn year -> four_factors_by_year(year) end)
  end

  defp four_factors_by_year(year) do
  end

  defp process_game(header, datetime) do
    season_start_year = header["SEASON"]
    season_end_year = season_year_suffix(season_start_year)
    gid = header["GAME_ID"]
    [_, game_code] = String.split(header["GAMECODE"], "/")
    year_month_day = Timex.format!(datetime, @s3_directory_format)
    s3_path = "#{season_start_year}/#{year_month_day}/#{gid}-#{game_code}.json"

    Process.sleep(1000)

    Nba.Stats.box_score(%{"GameID" => gid, "Season" => "#{season_start_year}-#{season_end_year}"})
    |> Poison.encode!()
    |> write_to_s3(s3_path)
  end

  def write_to_s3(text, path) do
    ExAws.S3.put_object(@s3_bucket_name, path, text)
    |> ExAws.request()
  end

  def aws_creds? do
    if Application.get_env(:ex_aws, :access_key_id) &&
         Application.get_env(:ex_aws, :secret_access_key) do
      :ok
    else
      :aws_error
    end
  end

  defp get_datetimes(start_date, end_date) do
    case {Timex.to_datetime(start_date), Timex.to_datetime(end_date)} do
      {{:error, _}, _} -> :date_error
      {_, {:error, _}} -> :date_error
      {sdt, edt} -> {sdt, edt}
    end
  end

  defp step_through_days(datetime, end_datetime) do
    case Timex.after?(datetime, end_datetime) do
      true ->
        IO.puts("Completed fetching box scores for the specified time range")

      false ->
        fetch_box_scores(datetime)
        step_through_days(Timex.add(datetime, Timex.Duration.from_days(1)), end_datetime)
    end
  end

  defp season_year_suffix(season_start_year) do
    (elem(Integer.parse(season_start_year), 0) + 1)
    |> Integer.to_string()
    |> String.slice(-2..-1)
  end
end
