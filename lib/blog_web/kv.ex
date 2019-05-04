defmodule BlogWeb.KV do
  @t %Tarantool.Simple{conn: Tarantool.Conn, schema: Tarantool.Schema}
  @kv %Tarantool.KV{iface: @t, space: :kv}

  alias Tarantool.KV, as: KV

  def get(key), do: KV.get(@kv, key)
  def set(key, val, options \\ []), do: KV.set(@kv, key, val, options)
  def add(key, val, options \\ []), do: KV.add(@kv, key, val, options)
  def delete(key), do: KV.delete(@kv, key)
  def incr(key, options \\ []), do: KV.incr(@kv, key, options)
end
