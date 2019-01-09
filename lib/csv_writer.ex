defmodule BigDataBaller.CsvWriter do

  def convert_all(start_year \\ 1996, end_year \\ 2018) do
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
    |> Enum.reduce([], fn file, acc ->
      IO.inspect(file)
      Enum.concat(acc, player_rows(file))
    end)
  end

  defp player_rows(file) do
    json_map =
      with {:ok, body} <- File.read(file),
           {:ok, json} <- Poison.decode(body),
           do: json

    Map.get(json_map, "PlayerStats")
    |> Enum.map(&player_map_to_list/1)
  end

  defp player_map_to_list(player_map) do
    [
      player_map["PLAYER_ID"],
      player_map["MIN"],
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
      player_map["GAME_ID"]
    ]
  end
end
