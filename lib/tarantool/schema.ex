defmodule Tarantool.Schema do
  @moduledoc """
  Module provides genserver which resolves space names into IDs in async,
  using provided tarantool connections or taking one from pool.

  Resolved names are being cached by the server.
  """

  @space_space 280
  @index_space_name 2

  @space_index 288
  @index_index_name 2

  # API

  def start_link(opts \\ []) do
    {pool, opts} = Keyword.pop(opts, :pool)
    {supervisor, opts} = Keyword.pop(opts, :supervisor)

    opts
    |> Keyword.put(:producer, {__MODULE__, :fetch, [pool]})
    |> Keyword.put(:tasks_supervisor, supervisor)
    |> Cache.start_link()
  end

  def child_spec(opts), do: %{Cache.child_spec(opts) | start: {__MODULE__, :start_link, [opts]}}

  def resolve_space(resolver, name), do: from_cached(Cache.fetch(resolver, {:space, name}))
  def resolve_space(resolver, name, t), do: from_cached(Cache.fetch(resolver, {:space, name}, t))

  def resolve_space!(resolver, name),
    do: force_success(resolve_space(resolver, name), {:space, name})

  def resolve_space!(resolver, name, t),
    do: force_success(resolve_space(resolver, name, t), {:space, name})

  def resolve_index(resolver, space, name),
    do: from_cached_2(resolve_space(resolver, space), &Cache.fetch(resolver, {:index, &1, name}))

  def resolve_index(resolver, space, name, t) do
    from_cached_2(
      resolve_space(resolver, space, t),
      &Cache.fetch(resolver, {:index, &1, name}, t)
    )
  end

  def resolve_index!(resolver, space, name),
    do: force_success(resolve_index(resolver, space, name), {:index, space, name})

  def resolve_index!(resolver, space, name, t),
    do: force_success(resolve_index(resolver, space, name, t), {:index, space, name})

  def fetch({:space, name}, t, _pool) do
    case Tarantool.Api.select(t, %{
           space_id: @space_space,
           index_id: @index_space_name,
           key: [name],
           iterator: nil,
           limit: 1,
           offset: 0
         }) do
      {:ok, [[id | _]]} ->
        {:ok, id}

      {:ok, []} ->
        :none
    end
  end

  def fetch({:index, space, name}, t, _pool) do
    case Tarantool.Api.select(t, %{
           space_id: @space_index,
           index_id: @index_index_name,
           key: [space, name],
           iterator: nil,
           limit: 1,
           offset: 0
         }) do
      {:ok, [[space_id | [index_id | _]]]} ->
        {:ok, {space_id, index_id}}

      {:ok, []} ->
        :none
    end
  end

  def fetch(name, pool) do
    :poolboy.transaction(pool, &fetch(name, &1, pool))
  end

  defp from_cached({:ok, :none}), do: :none
  defp from_cached({:ok, {:ok, id}}), do: {:ok, id}
  defp from_cached({:error, reason}), do: {:error, reason}

  defp from_cached_2({:ok, fst}, fun) do
    case fun.(fst) do
      {:ok, :none} -> :none
      {:ok, {:ok, id}} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  defp from_cached_2(fail, _), do: fail

  defp force_success(:none, {:space, name}), do: raise("space #{inspect(name)} not found")

  defp force_success(:none, {:index, space, name}),
    do: raise("index #{inspect(name)} not found in space #{inspect(space)}")

  defp force_success({:error, reason}, {:space, name}),
    do: raise("error while resolving space #{inspect(name)}: #{inspect(reason)}")

  defp force_success({:error, reason}, {:index, space, name}) do
    raise(
      "error while resolving index #{inspect(name)} in space #{inspect(space)}: #{inspect(reason)}"
    )
  end

  defp force_success({:ok, id}, _), do: id
end
