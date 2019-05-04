defmodule Cache do
  @moduledoc """
  Module acts as a caching server which can accept requests for computation
  results for given keys. If the computation for the key is already in progress,
  will not start the computation again. The computations for different keys
  are performed concurrently.
  """
  use GenServer

  # API

  def start_link(opts) do
    {producer, opts} = Keyword.pop(opts, :producer)
    {tasks_supervisor, opts} = Keyword.pop(opts, :tasks_supervisor)
    name = opts[:name]

    with {:ok, producer} <- resolve_producer(producer),
         {:ok, tasks_supervisor} <- resolve_supervisor(tasks_supervisor) do
      GenServer.start_link(__MODULE__, {producer, tasks_supervisor, name}, opts)
    end
  end

  @spec fetch_cached(pid(), binary()) :: {:ok, term()} | :progress | :miss
  def fetch_cached(pid, key) do
    case lookup(pid, key) do
      {:ok, val} -> {:ok, val}
      _ -> GenServer.call(pid, {:fetch_cached, key})
    end
  end

  @spec fetch(pid(), binary(), term()) :: {:ok, term()} | {:error, term()}
  def fetch(pid, key, param) do
    case lookup(pid, key) do
      {:ok, val} -> {:ok, val}
      _ -> GenServer.call(pid, {:fetch, key, param})
    end
  end

  @spec fetch(pid(), binary()) :: {:ok, term()} | {:error, term()}
  def fetch(pid, key) do
    case lookup(pid, key) do
      {:ok, val} -> {:ok, val}
      _ -> GenServer.call(pid, {:fetch, key})
    end
  end

  # Callbacks

  def init({producer, tasks_supervisor, name}) do
    cache_opts = [read_concurrency: true]
    cache_opts = if name == nil, do: cache_opts, else: cache_opts ++ [:named_table]
    cache = :ets.new(name || :cache, cache_opts)
    {:ok, {producer, tasks_supervisor, cache, %{}}}
  end

  def handle_call({:fetch_cached, key}, _from, {_, _, cache, _} = state) do
    case :ets.lookup(cache, key) do
      [{^key, {:value, value}}] ->
        {:reply, {:ok, value}, state}

      [{^key, {:progress, _}}] ->
        {:reply, :progress, state}

      [] ->
        {:reply, :miss, state}
    end
  end

  def handle_call({:fetch, key}, from, {producer, _, _, _} = state) do
    search(key, from, state, fn -> produce(producer, key) end)
  end

  def handle_call({:fetch, key, param}, from, {producer, _, _, _} = state) do
    search(key, from, state, fn -> produce(producer, key, param) end)
  end

  def handle_info({ref, res}, {producer, supervisor, cache, tasks}) do
    # Not interested in DOWN message
    Process.demonitor(ref, [:flush])

    {key, tasks} = Map.pop(tasks, ref)

    # Assert there was a query for this name
    [{^key, state}] = :ets.lookup(cache, key)

    with {:progress, pids} <- state do
      Enum.map(pids, &GenServer.reply(&1, {:ok, res}))
    end

    true = :ets.insert(cache, {key, {:value, res}})

    {:noreply, {producer, supervisor, cache, tasks}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, {producer, supervisor, cache, tasks}) do
    {key, tasks} = Map.pop(tasks, ref)
    [{^key, key_state}] = :ets.lookup(cache, key)
    true = :ets.delete(cache, key)

    with {:progress, pids} <- key_state do
      Enum.each(pids, &GenServer.reply(&1, {:error, reason}))
    end

    {:noreply, {producer, supervisor, cache, tasks}}
  end

  # Private

  defp search(key, from, {_, supervisor, cache, tasks} = state, fetcher) do
    case :ets.lookup(cache, key) do
      [{^key, {:value, value}}] ->
        {:reply, {:ok, value}, state}

      [{^key, {:progress, pids}}] ->
        true = :ets.insert(cache, {key, [from | pids]})
        {:noreply, state}

      [] ->
        %Task{ref: ref} = Task.Supervisor.async_nolink(supervisor, fetcher)
        true = :ets.insert(cache, {key, {:progress, [from]}})
        tasks = Map.put(tasks, ref, key)

        {:noreply, put_elem(state, 3, tasks)}
    end
  end

  defp resolve_producer(nil),
    do: {:error, "producer for cache values is required (:producer option)"}

  defp resolve_producer({mod, fun}), do: resolve_producer({mod, fun, []})
  defp resolve_producer({mod, fun, args}), do: {:ok, {:mfa, mod, fun, args}}
  defp resolve_producer(fun) when is_function(fun), do: {:ok, {:fun, fun}}

  defp resolve_supervisor(nil),
    do: {:error, "supervisor for tasks is required (:tasks_supervisor option)"}

  defp resolve_supervisor(pid) when is_pid(pid) or is_atom(pid), do: {:ok, pid}

  defp produce({:mfa, mod, fun, args}, key), do: apply(mod, fun, [key | args])
  defp produce({:fun, fun}, key), do: fun.(key)

  defp produce({:mfa, mod, fun, args}, key, param), do: apply(mod, fun, [key | [param | args]])
  defp produce({:fun, fun}, key, param), do: fun.(key, param)

  defp lookup(pid, key) when is_atom(pid) do
    with [{^key, {:value, val}}] <- :ets.lookup(pid, key), do: {:ok, val}
  end

  defp lookup(_, _), do: :error
end
