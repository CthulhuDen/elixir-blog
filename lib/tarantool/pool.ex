defmodule Tarantool.Pool do
  def child_spec(opts) do
    {name, _opts} = Keyword.pop(opts, :name)
    name = with _ when is_atom(name) <- name, do: {:local, name}

    :poolboy.child_spec(
      __MODULE__,
      name: name,
      worker_module: Tarantool.Conn,
      size: 10,
      max_overflow: 0,
      strategy: :fifo
    )
  end
end
