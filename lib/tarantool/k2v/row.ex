defmodule Tarantool.K2V.Row do
  alias Tarantool.K2V

  @enforce_keys [:kv, :key]
  defstruct [:kv, :key, options: [], loaded: {:some, []}, data: %{}]

  @type t :: %__MODULE__{}

  @spec get(K2V.t(), K2V.key(), K2V.opts()) :: {:ok, t()}
  def get(kv, key, options \\ []) do
    key = Tarantool.KV.normalize_str_key(key)

    {:ok, data} = K2V.get(kv, key, options)

    row = %__MODULE__{
      kv: kv,
      key: key,
      options: options,
      loaded: :all,
      data: data
    }

    {:ok, row}
  end

  @spec get_item(t(), K2V.key()) ::
          {:ok, K2V.val(), t()} | {:ok, K2V.val(), K2V.meta(), t()} | {:not_found, t()}
  def get_item(row, item_key) do
    item_key = Tarantool.KV.normalize_key(item_key)

    {:ok, row} =
      if !loaded?(row.loaded, item_key),
        do: get(row.kv, row.key, row.options),
        else: {:ok, row}

    case Map.fetch(row.data, item_key) do
      {:ok, val} ->
        if row.options[:include_meta] do
          {val, meta} = val
          {:ok, val, meta, row}
        else
          {:ok, val, row}
        end

      :error ->
        {:not_found, row}
    end
  end

  @spec set_item(t(), K2V.key(), K2V.val(), K2V.opts()) :: {:ok, K2V.meta(), t()}
  def set_item(row, item_key, val, options \\ []) do
    item_key = Tarantool.KV.normalize_key(item_key)

    {:ok, meta} = K2V.set_item(row.kv, row.key, item_key, val, options)

    val = if row.options[:include_meta], do: {val, meta}, else: val

    row = %{
      row
      | loaded: mark_loaded(row.loaded, item_key),
        data: Map.put(row.data, item_key, val)
    }

    {:ok, meta, row}
  end

  @spec add_item(t(), K2V.key(), K2V.val(), K2V.opts()) :: {:ok, K2V.meta(), t()} | {:exists, t()}
  def add_item(row, item_key, val, options \\ []) do
    item_key = Tarantool.KV.normalize_key(item_key)

    case Map.fetch(row.data, item_key) do
      {:ok, _} ->
        {:exists, row}

      :error ->
        case K2V.add_item(row.kv, row.key, item_key, val, options) do
          :exists ->
            {:exists, row}

          {:ok, meta} ->
            val = if row.options[:include_meta], do: {val, meta}, else: val

            row = %{
              row
              | loaded: mark_loaded(row.loaded, item_key),
                data: Map.put(row.data, item_key, val)
            }

            {:ok, meta, row}
        end
    end
  end

  @spec delete_item(t(), K2V.key()) :: {:ok, :not_found | {:deleted, K2V.val(), K2V.meta()}, t()}
  def delete_item(row, item_key) do
    item_key = Tarantool.KV.normalize_key(item_key)

    {:ok, res} = K2V.delete_item(row.kv, row.key, item_key)

    row = %{
      row
      | data: Map.delete(row.data, item_key),
        loaded: unmark_loaded(row.loaded, item_key)
    }

    {:ok, res, row}
  end

  @spec incr_item(t(), K2V.key(), K2V.opts()) ::
          {:ok, :added | :exited, number(), K2V.meta(), t()} | {:not_number, t()}
  def incr_item(row, item_key, options \\ []) do
    item_key = Tarantool.KV.normalize_key(item_key)

    case K2V.incr_item(row.kv, row.key, item_key, options) do
      :not_number ->
        {:not_number, row}

      {:ok, res, val, meta} ->
        full_val = if row.options[:include_meta], do: {val, meta}, else: val

        row = %{
          row
          | data: Map.put(row.data, item_key, full_val),
            loaded: mark_loaded(row.loaded, item_key)
        }

        {:ok, res, val, meta, row}
    end
  end

  defp loaded?({:some, fields}, field), do: field in fields
  defp loaded?(:all, _field), do: true

  defp mark_loaded({:some, fields}, field) do
    if field in fields,
      do: {:some, fields},
      else: {:some, [field | fields]}
  end

  defp mark_loaded(:all, _field), do: :all

  defp unmark_loaded({:some, fields}, field), do: {:some, Enum.reject(fields, &(&1 == field))}
  defp unmark_loaded(:all, _field), do: :all
end
