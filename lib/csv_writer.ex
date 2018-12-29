defmodule BigDataBaller.CsvWriter do
  
  def json_to_csv do 
    Path.wildcard("syncS3/2*/**/*.json") 
    |> Enum.each(&IO.puts(&1))
  end
end