defmodule Blog.Repo do
  use Ecto.Repo,
    otp_app: :blog,
    adapter: Tarantool.EctoAdapter
end
