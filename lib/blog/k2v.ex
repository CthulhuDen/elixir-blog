defmodule Blog.K2V do
  alias Tarantool.K2V, as: K2V

  @t %Tarantool.Simple{conn: Tarantool.Conn, schema: Tarantool.Schema}
  @kv %Tarantool.KV{iface: @t, space: :k2v}

  def get(key, options \\ []), do: K2V.get(@kv, key, options)

  def get_item(key, item_key), do: K2V.get_item(@kv, key, item_key)

  def set_item(key, item_key, val, options \\ []),
    do: K2V.set_item(@kv, key, item_key, val, options)

  def add_item(key, item_key, val, options \\ []),
    do: K2V.add_item(@kv, key, item_key, val, options)

  def delete(key, options \\ []), do: K2V.delete(@kv, key, options)

  def delete_item(key, item_key), do: K2V.delete_item(@kv, key, item_key)

  def incr_item(key, item_key, options \\ []), do: K2V.incr_item(@kv, key, item_key, options)

  def get_row(key, options \\ []), do: K2V.Row.get(@kv, key, options)
  def new_row(key, options \\ []), do: %K2V.Row{kv: @kv, key: key, options: options}
end
