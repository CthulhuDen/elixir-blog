defmodule Blog.User do
  use Ecto.Schema
  import Ecto.Changeset

  schema "users" do
    field(:login, :string)
    field(:password, :string)
  end

  @doc false
  def changeset(user, attrs) do
    user
    |> cast(attrs, [:login, :password])
    |> validate_required([:login])
  end
end
