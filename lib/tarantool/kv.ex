defmodule Tarantool.KV do
  @enforce_keys [:iface, :space]
  defstruct [:iface, :space]

  @type t :: %__MODULE__{}
  @type key :: term()
  @type val :: term()
  @type opts :: list({atom(), term()})

  @spec get(t(), key()) :: {:ok, val()} | :not_found
  def get(kv, key) do
    key = normalize_str_key(key)

    case Tarantool.Simple.select!(kv.iface, kv.space, :primary, [key]) do
      {:ok, []} -> :not_found
      {:ok, [[^key, val, ttl]]} -> {:ok, val, ttl: ttl}
      other -> other
    end
  end

  @spec set(t(), key(), val(), opts()) :: :ok
  def set(kv, key, val, options \\ []) do
    {ttl, []} = Keyword.pop(options, :ttl)
    ttl = normalize_ttl(ttl)
    key = normalize_str_key(key)

    with {:ok, [[^key, ^val, ^ttl]]} <-
           Tarantool.Simple.replace!(kv.iface, kv.space, [key, val, ttl]) do
      :ok
    end
  end

  @spec add(t(), key(), val(), opts()) :: :ok | :exists
  def add(kv, key, val, options \\ []) do
    {ttl, []} = Keyword.pop(options, :ttl)
    ttl = normalize_ttl(ttl)
    key = normalize_str_key(key)

    case Tarantool.Simple.insert!(kv.iface, kv.space, [key, val, ttl]) do
      {:error, 32771, _msg} -> :exists
      {:ok, [[^key, ^val, ^ttl]]} -> :ok
      other -> other
    end
  end

  @spec delete(t(), key()) :: {:ok, :not_found} | {:ok, {:deleted, val(), opts()}}
  def delete(kv, key) do
    key = normalize_str_key(key)

    case Tarantool.Simple.delete!(kv.iface, kv.space, :primary, [key]) do
      {:ok, []} -> {:ok, :not_found}
      {:ok, [[^key, val, ttl]]} -> {:ok, {:deleted, val, ttl: ttl}}
      other -> other
    end
  end

  @spec incr_nocheck(t(), key(), opts()) :: :ok
  def incr_nocheck(kv, key, options \\ []) do
    {default, options} = Keyword.pop(options, :default, 0)
    {delta, options} = Keyword.pop(options, :delta, 1)
    {ttl, options} = Keyword.pop(options, :ttl, nil)
    {update_ttl, []} = Keyword.pop(options, :update_ttl, false)

    ttl = normalize_ttl(ttl)
    ttl_ops = if update_ttl, do: [["=", ttl, 2]], else: []
    key = normalize_str_key(key)

    case Tarantool.Simple.upsert!(
           kv.iface,
           kv.space,
           [key, default + delta, ttl],
           [["+", 1, delta]] ++ ttl_ops
         ) do
      {:ok, []} -> :ok
      other -> other
    end
  end

  @spec incr(t(), key(), opts()) :: {:ok, :added | :exited, integer(), opts()} | :not_number
  def incr(kv, key, options \\ []) do
    {default, options} = Keyword.pop(options, :default, 0)
    {delta, options} = Keyword.pop(options, :delta, 1)
    {ttl, options} = Keyword.pop(options, :ttl, nil)
    {update_ttl, []} = Keyword.pop(options, :update_ttl, false)

    ttl = normalize_ttl(ttl)
    key = normalize_str_key(key)

    case Tarantool.Simple.call!(
           kv.iface,
           :incr,
           kv.space,
           [key],
           [delta, default, ttl, update_ttl]
         ) do
      {:ok, [[nil]]} -> {:ok, :added, default + delta, ttl: ttl}
      {:ok, [[val, ttl]]} -> {:ok, :existed, val, ttl: ttl}
      {:error, 32800, _msg} -> :not_number
      other -> other
    end
  end

  def normalize_ttl(ttl) do
    if is_integer(ttl) && ttl < 1_000_000_000,
      do: System.system_time(:second) + ttl,
      else: ttl
  end

  def normalize_str_key(key) do
    # TODO also handle numbers and shit (turn into strings)
    normalize_key(key)
  end

  def normalize_key(key) do
    cond do
      is_atom(key) -> Atom.to_string(key)
      true -> key
    end
  end
end
