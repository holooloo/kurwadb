defmodule Kurwa.Native do
  @moduledoc """
  The small amount of kurwadb that is written in Rust.

  What qualifies: a narrow interface, pure CPU, no I/O, no awareness of the
  cluster, and short enough never to hold a scheduler. Everything that makes
  this a *distributed* store stays on the BEAM, and deliberately so - the entire
  node-to-node protocol is under a hundred lines of Elixir because distribution,
  supervision and a process per connection come with the runtime. In Rust that
  would be thousands of lines, which is the wrong trade for the 80% of this
  codebase that is plumbing rather than arithmetic.

  So this is not a rewrite and is not meant to become one. It is the leaf
  operations where the BEAM's per-call overhead is most of the cost.

  ## Optional on purpose

  If `cargo` is not on the path when the project compiles, this module defines
  the same functions returning `:unavailable` and the callers keep their Elixir
  implementations. `mix test` passes either way, and both paths are checked
  against each other by the test suite.
  """

  @cargo? System.find_executable("cargo") != nil

  if @cargo? do
    use Rustler, otp_app: :kurwadb, crate: "kurwa_native"

    # Rustler replaces these at load time.
    @doc "Might `key` be in the filter? See `Kurwa.Store.Bloom`."
    def bloom_member(_filter, _bits, _hashes, _key), do: :erlang.nif_error(:nif_not_loaded)

    @doc "FNV-1a of `key`, 64 bit."
    def bloom_fnv1a(_key), do: :erlang.nif_error(:nif_not_loaded)
  else
    @doc false
    def bloom_member(_filter, _bits, _hashes, _key), do: :unavailable

    @doc false
    def bloom_fnv1a(_key), do: :unavailable
  end

  @doc "Was the native code compiled in?"
  @spec available?() :: boolean()
  def available?, do: @cargo?
end
