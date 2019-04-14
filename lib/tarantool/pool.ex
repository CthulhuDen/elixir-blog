defmodule Tarantool.Pool do
  def child_spec(_opts) do
    :poolboy.child_spec(
      __MODULE__,
      name: {:local, __MODULE__},
      worker_module: Tarantool.Conn,
      size: 10,
      max_overflow: 0,
      strategy: :fifo
    )
  end
end
