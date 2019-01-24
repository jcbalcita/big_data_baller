defmodule BigDataBaller.Team do
  def four_factors(start_year \\ 1996, end_year \\ 2019) do
    Enum.each(start_year..end_year, &four_factors_by_year/1)
  end

  defp four_factors_by_year(year) do
  end
end