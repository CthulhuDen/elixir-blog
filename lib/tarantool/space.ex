defmodule Tarantool.Space do
  @moduledoc """
  Module provides genserver which resolves space names into IDs in async,
  using provided tarantool connections or taking one from pool.

  Resolved names are being cached by the server.
  """

  @space_schema 280
  @index_schema_name 2

  @name __MODULE__

  use GenServer

  # API

  def start_link(opts \\ []) do
    opts = Keyword.put(opts, :name, @name)
    GenServer.start_link(__MODULE__, nil, opts)
  end

  @doc """
  Resolve a space name into ID, using connection from the main pool if needed.
  """
  @spec resolve(binary()) :: {:ok, integer()} | :none | :error
  def resolve(name) do
    GenServer.call(@name, {:resolve, name})
  end

  @doc """
  Resolve a space name into ID, using the provided connection if needed.
  """
  @spec resolve(pid(), binary()) :: {:ok, integer()} | :none | :error
  def resolve(t, name) do
    GenServer.call(@name, {:resolve_with, t, name})
  end

  # Callbacks

  def init(nil) do
    {:ok, {%{}, %{}}}
  end

  def handle_call({:resolve, name}, from, state) do
    search(name, from, state, fn -> fetch_id(name) end)
  end

  def handle_call({:resolve_with, t, name}, from, state) do
    search(name, from, state, fn -> fetch_id(t, name) end)
  end

  def handle_info({ref, res}, {names, tasks}) when is_reference(ref) do
    # Not interested in DOWN message
    Process.demonitor(ref, [:flush])

    {name, tasks} = Map.pop(tasks, ref)

    # Assert there was a query for this name
    {:ok, state} = Map.fetch(names, name)

    with {:progress, pids} <- state do
      Enum.map(pids, &GenServer.reply(&1, res))
    end

    names =
      case res do
        {:ok, id} -> Map.put(names, name, {:resolved, id})
        :none -> Map.delete(names, name)
      end

    {:noreply, {names, tasks}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {names, tasks}) do
    {name, tasks} = Map.pop(tasks, ref)
    {state, names} = Map.pop(names, name)

    with {:progress, pids} <- state do
      Enum.each(pids, &GenServer.reply(&1, :error))
    end

    {:noreply, {names, tasks}}
  end

  # Private

  defp search(name, from, {names, tasks}, fetcher) do
    case Map.fetch(names, name) do
      {:ok, {:resolved, id}} ->
        {:reply, {:ok, id}, {names, tasks}}

      {:ok, {:progress, pids}} ->
        {:noreply, {%{names | name => [from | pids]}, tasks}}

      :error ->
        %Task{ref: ref} = Task.Supervisor.async_nolink(Tarantool.Space.Supervisor, fetcher)
        names = Map.put(names, name, {:progress, [from]})
        tasks = Map.put(tasks, ref, name)
        {:noreply, {names, tasks}}
    end
  end

  defp fetch_id(name) do
    :poolboy.transaction(Tarantool.Pool, &fetch_id(&1, name))
  end

  defp fetch_id(t, name) do
    case Tarantool.Api.select(t, %{
           space_id: @space_schema,
           index_id: @index_schema_name,
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
end
