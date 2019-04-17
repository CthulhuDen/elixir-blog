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

    with {:ok, producer} <- resolve_producer(producer),
         {:ok, tasks_supervisor} <- resolve_supervisor(tasks_supervisor) do
      GenServer.start_link(__MODULE__, {producer, tasks_supervisor}, opts)
    end
  end

  @spec fetch_cached(pid(), binary()) :: {:ok, term()} | :progress | :miss
  def fetch_cached(pid, key) do
    GenServer.call(pid, {:fetch_cached, key})
  end

  @spec fetch(pid(), binary(), term()) :: {:ok, term()} | {:error, term()}
  def fetch(pid, key, param) do
    GenServer.call(pid, {:fetch, key, param})
  end

  @spec fetch(pid(), binary()) :: {:ok, term()} | {:error, term()}
  def fetch(pid, key) do
    GenServer.call(pid, {:fetch, key})
  end

  # Callbacks

  def init({producer, tasks_supervisor}) do
    {:ok, {producer, tasks_supervisor, %{}, %{}}}
  end

  def handle_call({:fetch_cached, key}, _from, {_, _, cache, _} = state) do
    case Map.fetch(cache, key) do
      {:ok, {:value, value}} ->
        {:reply, {:ok, value}, state}

      {:ok, {:progress, _}} ->
        {:reply, :progress, state}

      :error ->
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
    {:ok, state} = Map.fetch(cache, key)

    with {:progress, pids} <- state do
      Enum.map(pids, &GenServer.reply(&1, {:ok, res}))
    end

    {:noreply, {producer, supervisor, %{cache | key => {:value, res}}, tasks}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, {producer, supervisor, cache, tasks}) do
    {key, tasks} = Map.pop(tasks, ref)
    {key_state, cache} = Map.pop(cache, key)

    with {:progress, pids} <- key_state do
      Enum.each(pids, &GenServer.reply(&1, {:error, reason}))
    end

    {:noreply, {producer, supervisor, cache, tasks}}
  end

  # Private

  defp search(key, from, {producer, supervisor, cache, tasks} = state, fetcher) do
    case Map.fetch(cache, key) do
      {:ok, {:value, value}} ->
        {:reply, {:ok, value}, state}

      {:ok, {:progress, pids}} ->
        {:noreply, put_elem(state, 2, %{cache | key => [from | pids]})}

      :error ->
        %Task{ref: ref} = Task.Supervisor.async_nolink(supervisor, fetcher)
        cache = Map.put(cache, key, {:progress, [from]})
        tasks = Map.put(tasks, ref, key)

        {:noreply, {producer, supervisor, cache, tasks}}
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
end
