defmodule BlogWeb.CounterController do
  use BlogWeb, :controller
  alias Blog.KV, as: KV

  def index(conn, _params) do
    {:ok, _, counter, _} = KV.incr(:counter, ttl: 14)

    conn
    |> assign(:counter, counter)
    |> render("index.html")
  end
end
