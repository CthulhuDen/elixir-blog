defmodule Tarantool.K2V do
  @enforce_keys [:iface, :space]
  defstruct [:iface, :space]

  @type t :: %__MODULE__{}
  @type key :: term()
  @type val :: term()
  @type opts :: list({atom(), term()})
  @type ttl :: integer() | nil
  @type meta :: %{required(:ttl) => ttl(), optional(atom()) => term()}
  @type item :: {val(), meta()}
  @type row :: %{required(key()) => item()}

  @spec get(t(), key()) :: {:ok, row()} | :not_found
  def get(kv, key) do
    key = Tarantool.KV.normalize_str_key(key)

    case Tarantool.Simple.select!(kv.iface, kv.space, :primary, [key], limit: 999) do
      {:ok, []} ->
        :not_found

      {:ok, items} ->
        row =
          items
          |> Enum.map(fn [^key, item_key, val, ttl] -> {item_key, {val, %{ttl: ttl}}} end)
          |> Enum.into(%{})

        {:ok, row}

      other ->
        other
    end
  end

  @spec get_item(t(), key(), key()) :: {:ok, val(), meta()} | :not_found
  def get_item(kv, key, item_key) do
    key = Tarantool.KV.normalize_str_key(key)
    item_key = Tarantool.KV.normalize_key(item_key)

    case Tarantool.Simple.select!(kv.iface, kv.space, :primary, [key, item_key]) do
      {:ok, []} ->
        :not_found

      {:ok, [[^key, ^item_key, val, ttl]]} ->
        {:ok, val, %{ttl: ttl}}

      other ->
        other
    end
  end

  @spec set_item(t(), key(), key(), val(), opts()) :: :ok
  def set_item(kv, key, item_key, val, options \\ []) do
    {ttl, []} = Keyword.pop(options, :ttl)
    ttl = Tarantool.KV.normalize_ttl(ttl)
    key = Tarantool.KV.normalize_str_key(key)
    item_key = Tarantool.KV.normalize_key(item_key)

    with {:ok, [[^key, ^item_key, ^val, ^ttl]]} <-
           Tarantool.Simple.replace!(kv.iface, kv.space, [key, item_key, val, ttl]) do
      :ok
    end
  end

  @spec add_item(t(), key(), key, val(), opts()) :: :ok | :exists
  def add_item(kv, key, item_key, val, options \\ []) do
    {ttl, []} = Keyword.pop(options, :ttl)
    ttl = Tarantool.KV.normalize_ttl(ttl)
    key = Tarantool.KV.normalize_str_key(key)
    item_key = Tarantool.KV.normalize_key(item_key)

    case Tarantool.Simple.insert!(kv.iface, kv.space, [key, item_key, val, ttl]) do
      {:error, 32771, _msg} -> :exists
      {:ok, [[^key, ^item_key, ^val, ^ttl]]} -> :ok
      other -> other
    end
  end

  @spec delete(t(), key()) :: {:ok, :not_found} | {:ok, {:deleted, row()}}
  @doc """
  Not actually working. TODO
  """
  def delete(kv, key) do
    key = Tarantool.KV.normalize_str_key(key)

    case Tarantool.Simple.delete!(kv.iface, kv.space, :primary, [key]) do
      {:ok, []} ->
        {:ok, :not_found}

      {:ok, items} ->
        row =
          items
          |> Enum.map(fn [^key, item_key, val, ttl] -> {item_key, {val, %{ttl: ttl}}} end)
          |> Enum.into(%{})

        {:ok, {:deleted, row}}

      other ->
        other
    end
  end

  @spec delete_item(t(), key(), key()) :: {:ok, :not_found} | {:ok, {:deleted, val(), meta()}}
  def delete_item(kv, key, item_key) do
    key = Tarantool.KV.normalize_str_key(key)
    item_key = Tarantool.KV.normalize_key(item_key)

    case Tarantool.Simple.delete!(kv.iface, kv.space, :primary, [key, item_key]) do
      {:ok, []} ->
        {:ok, :not_found}

      {:ok, [[^key, ^item_key, val, ttl]]} ->
        {:ok, {:deleted, val, %{ttl: ttl}}}

      other ->
        other
    end
  end

  @spec incr_item(t(), key(), key(), opts()) ::
          {:ok, :added | :exited, number(), meta()} | :not_number
  def incr_item(kv, key, item_key, options \\ []) do
    {default, options} = Keyword.pop(options, :default, 0)
    {delta, options} = Keyword.pop(options, :delta, 1)
    {ttl, options} = Keyword.pop(options, :ttl, nil)
    {update_ttl, []} = Keyword.pop(options, :update_ttl, false)

    ttl = Tarantool.KV.normalize_ttl(ttl)
    key = Tarantool.KV.normalize_str_key(key)
    item_key = Tarantool.KV.normalize_key(item_key)

    case Tarantool.Simple.call!(
           kv.iface,
           :incr,
           kv.space,
           [key, item_key],
           [delta, default, ttl, update_ttl]
         ) do
      {:ok, [[nil]]} -> {:ok, :added, default + delta, %{ttl: ttl}}
      {:ok, [[val, ttl]]} -> {:ok, :existed, val, %{ttl: ttl}}
      {:error, 32800, _msg} -> :not_number
      other -> other
    end
  end

  def strip_meta(row) do
    row
    |> Enum.map(fn {key, {val, _meta}} -> {key, val} end)
    |> Enum.into(%{})
  end
end
