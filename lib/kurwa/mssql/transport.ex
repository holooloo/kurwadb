defmodule Kurwa.Mssql.Transport do
  @moduledoc """
  The socket `:ssl` sees on a TDS connection.

  TDS 7.x negotiates TLS inside its own framing: the handshake records travel
  as the payload of PRELOGIN packets, both ways, and only once it is done do
  TLS records go over the TCP stream bare. `:ssl` knows nothing of that, but it
  accepts any transport that behaves like `gen_tcp` (`cb_info`), so this is one:
  a process that owns the TCP socket and stands between it and `:ssl`.

  Coming in, it tells the two apart by the first byte - a TDS PRELOGIN packet
  starts with 0x12, a TLS record with 0x14..0x17 - and hands `:ssl` the TLS
  bytes either way. Going out, it wraps them in PRELOGIN packets until told the
  handshake is over (`raw/1`), then passes them through.

  TDS 8 ("strict" encryption) starts TLS before anything else, with no
  wrapping at all; the same process serves it with `wrap: false` from the
  start. And for "login-only" encryption, where only LOGIN7 is encrypted and
  the connection then carries on in the clear, `release/1` hands the TCP socket
  back without a TLS close reaching the client.
  """

  use GenServer

  # The gen_tcp interface needs a send/2 of its own.
  import Kernel, except: [send: 2]

  @data :kurwa_tds_tls
  @closed :kurwa_tds_tls_closed
  @error :kurwa_tds_tls_error
  @passive :kurwa_tds_tls_passive

  @doc "The `cb_info` to give `:ssl` for a socket made by `start/3`."
  def cb_info, do: {__MODULE__, @data, @closed, @error, @passive}

  @doc "Takes over `socket` (call from its owner), with `buffer` already read from it."
  def start(socket, buffer, wrap?) do
    {:ok, pid} = GenServer.start(__MODULE__, {socket, buffer, wrap?, self()})
    :ok = :gen_tcp.controlling_process(socket, pid)
    GenServer.call(pid, :listen)
    {:ok, pid}
  end

  @doc "The handshake is over: stop wrapping what goes out."
  def raw(pid), do: GenServer.call(pid, :raw)

  @doc """
  Returns the TCP socket to the caller, passive, after closing `tls` without
  a byte of it reaching the client: once released, whatever TLS still sends -
  its close alert included - is dropped.
  """
  def release(pid, tls) do
    socket = GenServer.call(pid, {:release, self()})
    _ = :ssl.close(tls)
    GenServer.stop(pid)
    socket
  end

  # ------------------------------------------------- the gen_tcp interface

  def send(pid, data), do: GenServer.call(pid, {:send, data})
  def recv(pid, _length, timeout), do: GenServer.call(pid, :recv, timeout_ms(timeout))
  def recv(pid, length), do: recv(pid, length, :infinity)
  def setopts(pid, opts), do: GenServer.call(pid, {:setopts, opts})
  def getopts(pid, opts), do: GenServer.call(pid, {:getopts, opts})
  def controlling_process(pid, owner), do: GenServer.call(pid, {:owner, owner})
  def peername(pid), do: GenServer.call(pid, :peername)
  def sockname(pid), do: GenServer.call(pid, :sockname)
  def port(_pid), do: {:ok, 0}
  def getstat(pid, opts), do: GenServer.call(pid, {:getstat, opts})
  def close(pid), do: if(Process.alive?(pid), do: GenServer.call(pid, :close), else: :ok)
  def shutdown(_pid, _how), do: :ok

  defp timeout_ms(:infinity), do: :infinity
  defp timeout_ms(ms), do: ms + 1_000

  # ---------------------------------------------------------------- server

  @impl true
  def init({socket, buffer, wrap?, owner}) do
    {:ok,
     %{
       socket: socket,
       owner: owner,
       wrap: wrap?,
       buffer: buffer,
       queue: :queue.new(),
       active: false,
       waiter: nil,
       dropping: false
     }}
  end

  @impl true
  def handle_call(:listen, _from, state) do
    :inet.setopts(state.socket, active: :once)
    {:reply, :ok, unwrap(state)}
  end

  def handle_call(:raw, _from, state), do: {:reply, :ok, %{state | wrap: false}}

  def handle_call({:release, owner}, _from, state) do
    :ok = :inet.setopts(state.socket, active: false)
    :ok = :gen_tcp.controlling_process(state.socket, owner)
    {:reply, state.socket, %{state | dropping: true}}
  end

  def handle_call({:send, _data}, _from, %{dropping: true} = state), do: {:reply, :ok, state}

  def handle_call({:send, data}, _from, state) do
    data = IO.iodata_to_binary(data)
    out = if state.wrap, do: Kurwa.Mssql.Tds.packets(0x12, data, 4096), else: data
    {:reply, :gen_tcp.send(state.socket, out), state}
  end

  def handle_call(:recv, from, state) do
    case :queue.out(state.queue) do
      {{:value, data}, queue} -> {:reply, {:ok, data}, %{state | queue: queue}}
      {:empty, _} -> {:noreply, %{state | waiter: from}}
    end
  end

  def handle_call({:setopts, opts}, _from, state) do
    active =
      Enum.find_value(opts, state.active, fn
        {:active, value} -> value
        _ -> nil
      end)

    {:reply, :ok, deliver(%{state | active: active})}
  end

  def handle_call({:getopts, opts}, _from, state) do
    known = %{
      active: state.active,
      mode: :binary,
      packet: 0,
      packet_size: 0,
      header: 0,
      deliver: :term
    }

    {:reply, {:ok, for(opt <- opts, Map.has_key?(known, opt), do: {opt, known[opt]})}, state}
  end

  def handle_call({:owner, owner}, _from, state), do: {:reply, :ok, %{state | owner: owner}}
  def handle_call(:peername, _from, state), do: {:reply, :inet.peername(state.socket), state}
  def handle_call(:sockname, _from, state), do: {:reply, :inet.sockname(state.socket), state}

  def handle_call({:getstat, opts}, _from, state),
    do: {:reply, :inet.getstat(state.socket, opts), state}

  def handle_call(:close, _from, %{dropping: true} = state), do: {:reply, :ok, state}

  def handle_call(:close, _from, state) do
    :gen_tcp.close(state.socket)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:tcp, socket, data}, %{socket: socket, dropping: false} = state) do
    :inet.setopts(socket, active: :once)
    {:noreply, %{state | buffer: state.buffer <> data} |> unwrap() |> deliver()}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    Kernel.send(state.owner, {@closed, self()})
    {:noreply, state}
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    Kernel.send(state.owner, {@error, self(), reason})
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # Moves complete units from the byte buffer to the delivery queue: a whole
  # PRELOGIN packet's payload, or whatever bare TLS bytes there are.
  defp unwrap(%{buffer: <<0x12, _status, len::16, _::32, _::binary>> = buffer} = state)
       when byte_size(buffer) >= len and len >= 8 do
    payload_len = len - 8
    <<_header::binary-size(8), payload::binary-size(^payload_len), rest::binary>> = buffer
    unwrap(%{state | buffer: rest, queue: :queue.in(payload, state.queue)})
  end

  defp unwrap(%{buffer: <<0x12, _::binary>>} = state), do: state
  defp unwrap(%{buffer: <<>>} = state), do: state
  defp unwrap(state), do: %{state | buffer: <<>>, queue: :queue.in(state.buffer, state.queue)}

  defp deliver(%{waiter: from} = state) when from != nil do
    case :queue.out(state.queue) do
      {{:value, data}, queue} ->
        GenServer.reply(from, {:ok, data})
        deliver(%{state | queue: queue, waiter: nil})

      {:empty, _} ->
        state
    end
  end

  defp deliver(%{active: active} = state) when active in [false, 0], do: state

  defp deliver(state) do
    case :queue.out(state.queue) do
      {{:value, data}, queue} ->
        Kernel.send(state.owner, {@data, self(), data})

        active =
          case state.active do
            :once ->
              false

            true ->
              true

            1 ->
              Kernel.send(state.owner, {@passive, self()})
              0

            n when is_integer(n) ->
              n - 1
          end

        deliver(%{state | queue: queue, active: active})

      {:empty, _} ->
        state
    end
  end
end
