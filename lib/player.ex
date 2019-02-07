defmodule BigDataBaller.Player do
  require Logger
  alias BigDataBaller.Util

  def all_game_logs() do
    with s3_path <- "player/game_log/game_logs_all.json",
         {:ok, result} <- Nba.Stats.player_game_logs(%{"Season" => ""}) do
      Jason.encode!(result)
      |> Util.write_to_s3(s3_path)
    else 
      {:error, message} -> Logger.error(message)
    end
  end
end
