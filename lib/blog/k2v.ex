defmodule Blog.K2V do
  @t %Tarantool.Simple{conn: Tarantool.Conn, schema: Tarantool.Schema}
  @kv %Tarantool.KV{iface: @t, space: :k2v}

  alias Tarantool.K2V, as: K2V

  def get(key), do: K2V.get(@kv, key)

  def get_vals(key) do
    with {:ok, row} <- K2V.get(@kv, key), do: {:ok, K2V.strip_meta(row)}
  end

  def get_item(key, item_key), do: K2V.get_item(@kv, key, item_key)

  def set_item(key, item_key, val, options \\ []),
    do: K2V.set_item(@kv, key, item_key, val, options)

  def add_item(key, item_key, val, options \\ []),
    do: K2V.add_item(@kv, key, item_key, val, options)

  def delete(key), do: K2V.delete(@kv, key)

  def delete_vals(key) do
    with {:ok, {:deleted, row}} <- K2V.delete(@kv, key),
         do: {:ok, {:deleted, K2V.strip_meta(row)}}
  end

  def delete_item(key, item_key), do: K2V.delete_item(@kv, key, item_key)
  def incr_item(key, item_key, options \\ []), do: K2V.incr_item(@kv, key, item_key, options)
end
