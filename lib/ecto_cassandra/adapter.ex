defmodule EctoCassandra.Adapter do
  @moduledoc """
  Ecto 2.x adapter for the Cassandra database
  """

  @adapter EctoCassandra.Planner
  @storage_adapter EctoCassandra.Storage
  @migration_adapter EctoCassandra.Migration
  @structure_adapter EctoCassandra.Structure

  alias Xandra.Batch

  @behaviour Ecto.Adapter

  @doc false
  defmacro __before_compile__(_env), do: :ok

  @doc false
  defdelegate ensure_all_started(config, type), to: @adapter

  @doc false
  defdelegate init(config), to: @adapter

  @doc false
  defdelegate checkout(adapter_meta, config, function), to: @adapter

  @doc false
  defdelegate loaders(primitive_type, ecto_type), to: @adapter

  @doc false
  defdelegate dumpers(primitive_type, ecto_type), to: @adapter


  @behaviour Ecto.Adapter.Queryable

  @doc false
  defdelegate prepare(operation, query), to: @adapter

  @doc false
  defdelegate execute(adapter_meta, query_meta, query_cache, params, options), to: @adapter

  @doc false
  defdelegate stream(adapter_meta, query_meta, query_cache, params, options), to: @adapter


  @behaviour Ecto.Adapter.Schema

  @doc false
  defdelegate autogenerate(field_type), to: @adapter

  @doc false
  defdelegate insert_all(adapter_meta, schema_meta, header, rows, on_conflict, returning, options),
    to: @adapter

  @doc false
  defdelegate insert(adapter_meta, schema_meta, fields, on_conflict, returning, options), to: @adapter

  @doc false
  defdelegate update(adapter_meta, schema_meta, fields, filter, returning, options), to: @adapter

  @doc false
  defdelegate delete(adapter_meta, schema_meta, filters, options), to: @adapter


  @behaviour Ecto.Adapter.Storage

  @doc false
  defdelegate storage_down(options), to: @storage_adapter

  @doc false
  defdelegate storage_up(options), to: @storage_adapter


  @behaviour Ecto.Adapter.Transaction

  @doc false
  @spec transaction(any, any, any) :: nil
  def transaction(_adapter_meta, _options, _function), do: nil

  @doc false
  @spec in_transaction?(any) :: false
  def in_transaction?(_adapter_meta), do: false

  @doc false
  @spec rollback(any, any) :: nil
  def rollback(_adapter_meta, _value), do: nil

  @doc """
  Cassandra batches

  Accepts list of statements and running these queries in a batch.
  Returns `{:ok, Xandra.Void.t}` or `{:error, any}`

  Example:

  ```
  EctoCassandra.Adapter.batch([%User{} |> User.changeset(attrs) |> Repo.insert(execute: false),
    %User{} |> User.changeset(another_attrs) |> Repo.insert(execute: false)
  ])
  ```
  """
  @spec batch([String.t()]) :: {:ok, Xandra.Void.t()} | {:error, any}
  def batch(queries) do
    batch =
      Enum.reduce(queries, Batch.new(:logged), fn q, acc ->
        apply(Batch, :add, [acc] ++ [q])
      end)

    Xandra.execute(EctoCassandra.Conn, batch)
  end

  @behaviour Ecto.Adapter.Migration

  @doc false
  def supports_ddl_transaction?, do: false

  @doc false
  defdelegate execute_ddl(adapter_meta, command, options), to: @migration_adapter

  @doc false
  @spec lock_for_migrations(
          Ecto.Adapter.adapter_meta(),
          Ecto.Query.t(),
          options :: Keyword.t(),
          fun
        ) :: no_return
  def lock_for_migrations(_adapter_meta, _query, _options, _function),
    do: raise(RuntimeError, "Can't lock the migrations tables")

  @behaviour Ecto.Adapter.Structure

  @doc false
  defdelegate structure_dump(default, config), to: @structure_adapter

  @doc false
  defdelegate structure_load(default, config), to: @structure_adapter
end
