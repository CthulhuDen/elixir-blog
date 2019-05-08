defmodule BlogWeb.CounterController do
  use BlogWeb, :controller
  alias Blog.K2V
  alias Tarantool.K2V.Row, as: KVRow

  def index(conn, _params) do
    {:ok, locker} = K2V.get_row(:counter)

    {incremented, counter} =
      case KVRow.add_item(locker, :lock, nil, ttl: 10) do
        {:ok, _, locker} ->
          {:ok, _, counter, _, _locker} = KVRow.incr_item(locker, :value, update_ttl: true)
          {true, counter}

        {:exists, locker} ->
          {:ok, counter, _locker} = KVRow.get_item(locker, :value)
          {false, counter}
      end

    conn
    |> render("index.html", incremented: incremented, counter: counter)
  end
end
