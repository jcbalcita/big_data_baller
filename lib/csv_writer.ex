defmodule BigDataBaller.CsvWriter do
  def convert(start_year \\ 1996, end_year \\ 2018) do
    start_year..end_year
    |> Enum.each(&json_to_csv/1)
  end

  def json_to_csv(year) do
    File.mkdir("spark/csv")
    File.touch("spark/csv/#{year}.csv")
    {:ok, file} = File.open("spark/csv/#{year}.csv", [:write, :utf8])

    Path.wildcard("syncS3/#{year}/**/*.json")
    |> convert_to_matrix()
    |> CSV.encode()
    |> Enum.each(&IO.write(file, &1))

    File.close(file)
  end

  defp convert_to_matrix(filepaths) do
    filepaths
    |> Enum.reduce([], fn filepath, acc ->
      Enum.concat(acc, player_rows(filepath))
    end)
  end

  defp player_rows(filepath) do
    with {:ok, body} <- File.read(filepath),
         {:ok, json} <- Poison.decode(body),
         home_away_names <- get_home_and_away_teams(filepath),
         season <- get_season_year(filepath) do
      create_rows(json, season, home_away_names)
    else
      _ -> IO.puts("[ERROR] Unable to ro read #{filepath}")
    end
  end

  def create_rows(box_score, season, home_away_names) do
    player_stats = Map.get(box_score, "PlayerStats")
    team_stats = Map.get(box_score, "TeamStats")
    team_home_away_info = get_team_home_away_status(team_stats, home_away_names)

    player_stats
    |> Enum.map(fn player_map ->
      create_row(player_map, season, team_home_away_info)
    end)
  end

  def create_row(player_map, season, [{h_id, h_name}, {a_id, _}]) do
    player_stats = get_player_stats(player_map, h_name)
    opposing_team_stats = get_opposing_team_stats(player_map, season, {h_id, a_id})

    Enum.concat(player_stats, opposing_team_stats)
  end

  def get_player_stats(player_map, home_team_name) do
    minutes =
      if player_map["MIN"],
        do: String.split(player_map["MIN"], ":") |> List.first() |> Integer.parse() |> elem(0),
        else: nil

    [
      player_map["PLAYER_ID"],
      minutes,
      player_map["PTS"],
      player_map["OREB"],
      player_map["DREB"],
      player_map["REB"],
      player_map["AST"],
      player_map["STL"],
      player_map["BLK"],
      player_map["TO"],
      player_map["FTM"],
      player_map["FTA"],
      player_map["FGM"],
      player_map["FGA"],
      player_map["FG3M"],
      player_map["FG3A"],
      player_map["PLAYER_NAME"],
      player_map["TEAM_ID"],
      player_map["TEAM_ABBREVIATION"],
      player_map["START_POSITION"],
      player_map["PLUS_MINUS"],
      player_map["GAME_ID"],
      player_map["TEAM_ABBREVIATION"] == home_team_name
    ]
  end

  def get_home_and_away_teams(filepath) do
    String.split(filepath, "/")
    |> List.last()
    |> String.split("-")
    |> List.last()
    |> String.split(".")
    |> List.first()
    |> String.split_at(3)
  end

  def get_team_home_away_status(team_stats, {home_name, away_name}) do
    home_id =
      team_stats
      |> Enum.find(fn team -> team["TEAM_ABBREVIATION"] == home_name end)
      |> Map.get("TEAM_ID")

    away_id =
      team_stats
      |> Enum.find(fn team -> team["TEAM_ABBREVIATION"] != home_name end)
      |> Map.get("TEAM_ID")

    [{home_id, home_name}, {away_id, away_name}]
  end

  defp get_opposing_team_stats(player_map, season, {home_team_id, away_team_id}) do

  end

  def get_season_year(filepath) do
    season_start_year = filepath |> String.split("/") |> Enum.at(1) |> Integer.parse()
    season_end_year = (season_start_year + 1) |> Integer.to_string() |> String.slice(2)

    season_start_year <> "-" <> season_end_year
  end
end
