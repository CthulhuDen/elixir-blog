defmodule Tarantool.Conn do
  def start_link(opts \\ []) do
    ret = Tarantool.start_link()

    case {ret, Keyword.fetch(opts, :name)} do
      {{:ok, _}, :error} -> {}
      {{:ok, pid}, {:ok, name}} -> Process.register(pid, name)
    end

    ret
  end

  def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
end
