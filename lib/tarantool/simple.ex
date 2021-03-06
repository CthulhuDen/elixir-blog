defmodule Tarantool.Simple do
  @debug false

  @enforce_keys [:conn, :schema]
  defstruct [:conn, :schema]

  def iterator_all, do: 2
  def iterator_eq, do: 0
  def iterator_ge, do: 5
  def iterator_gt, do: 6
  def iterator_le, do: 4
  def iterator_lt, do: 3

  @defaults %{
    limit: 1,
    offset: 0,
    iterator: nil
  }

  def select!(t, space, index \\ 0, key \\ [], params \\ []) do
    if @debug, do: IO.puts("SELECT FROM #{space} USING INDEX #{index}")

    {space_id, index_id} = Tarantool.Schema.resolve_index!(t.schema, space, index)

    params =
      params
      |> Keyword.merge(space_id: space_id, index_id: index_id, key: key)
      |> Keyword.merge(if key == [], do: [iterator: iterator_all()], else: [])
      |> Enum.into(@defaults)

    Tarantool.Api.select(t.conn, params)
  end

  def insert!(%__MODULE__{conn: conn, schema: schema}, space, tuple) do
    if @debug, do: IO.puts("INSERT INTO SPACE #{space}")

    space_id = Tarantool.Schema.resolve_space!(schema, space)

    Tarantool.Api.insert(conn, %{space_id: space_id, tuple: tuple})
  end

  def update!(%__MODULE__{conn: conn, schema: schema}, space, index, key, operations) do
    if @debug, do: IO.puts("UPDATE SPACE #{space} USING INDEX #{index}")

    {space_id, index_id} = Tarantool.Schema.resolve_index!(schema, space, index)

    Tarantool.Api.update(conn, %{
      space_id: space_id,
      index_id: index_id,
      key: key,
      tuple: operations
    })
  end

  def upsert!(%__MODULE__{conn: conn, schema: schema}, space, tuple, operations) do
    if @debug, do: IO.puts("UPSERT INTO SPACE #{space}")

    space_id = Tarantool.Schema.resolve_space!(schema, space)

    Tarantool.Api.upsert(conn, %{space_id: space_id, tuple: tuple, ops: operations})
  end

  def replace!(%__MODULE__{conn: conn, schema: schema}, space, tuple) do
    if @debug, do: IO.puts("REPLACE INTO SPACE #{space}")

    space_id = Tarantool.Schema.resolve_space!(schema, space)

    Tarantool.Api.replace(conn, %{space_id: space_id, tuple: tuple})
  end

  def delete!(%__MODULE__{conn: conn, schema: schema}, space, index, key) do
    if @debug, do: IO.puts("DELETE FROM SPACE #{space} USING INDEX #{index}")

    {space_id, index_id} = Tarantool.Schema.resolve_index!(schema, space, index)

    Tarantool.Api.delete(conn, %{space_id: space_id, index_id: index_id, key: key})
  end

  def call!(%__MODULE__{conn: conn, schema: schema}, fun, space, key, rest \\ []) do
    if @debug, do: IO.puts("CALLING FUNCTION #{fun} OVER SPACE #{space}")

    space_id = Tarantool.Schema.resolve_space!(schema, space)

    Tarantool.Api.call(conn, %{function_name: fun, tuple: [space_id, key] ++ rest})
  end
end
