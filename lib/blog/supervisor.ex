defmodule Blog.Supervisor do
  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args)
  end

  def init({children, opts}) do
    Supervisor.init(children, opts)
  end
end
