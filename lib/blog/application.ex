defmodule Blog.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    Supervisor.start_link(
      [
        BlogWeb.Endpoint,
        {Tarantool.Schema,
         name: Tarantool.Schema, conn: Tarantool.Conn, supervisor: Tarantool.Schema.Supervisor},
        {Tarantool.Conn, name: Tarantool.Conn},
        {Task.Supervisor, name: Tarantool.Schema.Supervisor}
      ],
      strategy: :one_for_one,
      name: Blog.Supervisor
    )
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  def config_change(changed, _new, removed) do
    BlogWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
