defmodule Tarantool.Simple do
  @enforce_keys [:conn, :schema]
  defstruct [:conn, :schema]

  @defaults %{
    limit: 1,
    offset: 0,
    iterator: nil
  }

  def select!(t, space, index \\ 0, key \\ [], params \\ []) do
    {space_id, index_id} = Tarantool.Schema.resolve_index!(t.schema, space, index)

    params =
      params
      |> Keyword.merge(space_id: space_id, index_id: index_id, key: key)
      |> Enum.into(@defaults)

    Tarantool.Api.select(t.conn, params)
  end

  def insert!(%__MODULE__{conn: conn, schema: schema}, space, tuple) do
    space_id = Tarantool.Schema.resolve_space!(schema, space)

    Tarantool.Api.insert(conn, %{space_id: space_id, tuple: tuple})
  end

  def update!(%__MODULE__{conn: conn, schema: schema}, space, index, key, operations) do
    {space_id, index_id} = Tarantool.Schema.resolve_index!(schema, space, index)

    Tarantool.Api.update(conn, %{
      space_id: space_id,
      index_id: index_id,
      key: key,
      tuple: operations
    })
  end

  def upsert!(%__MODULE__{conn: conn, schema: schema}, space, tuple, operations) do
    space_id = Tarantool.Schema.resolve_space!(schema, space)

    Tarantool.Api.upsert(conn, %{space_id: space_id, tuple: tuple, ops: operations})
  end

  def replace!(%__MODULE__{conn: conn, schema: schema}, space, tuple) do
    space_id = Tarantool.Schema.resolve_space!(schema, space)

    Tarantool.Api.replace(conn, %{space_id: space_id, tuple: tuple})
  end

  def delete!(%__MODULE__{conn: conn, schema: schema}, space, index, key) do
    {space_id, index_id} = Tarantool.Schema.resolve_index!(schema, space, index)

    Tarantool.Api.delete(conn, %{space_id: space_id, index_id: index_id, key: key})
  end

  def call!(%__MODULE__{conn: conn, schema: schema}, fun, space, key, rest \\ []) do
    space_id = Tarantool.Schema.resolve_space!(schema, space)

    Tarantool.Api.call(conn, %{function_name: fun, tuple: [space_id, key] ++ rest})
  end
end
