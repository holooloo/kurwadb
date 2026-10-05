defmodule Kurwa.Pg.Proto do
  @moduledoc """
  The PostgreSQL frontend/backend protocol, version 3.0: framing and the
  messages kurwadb sends and understands.

  Every message after startup is a type byte, a 32-bit length that counts
  itself but not the type, and a body. The startup packet has no type byte,
  which is why it is decoded separately (`decode_startup/1`).

  Only what a server needs is here: decoding frontend messages, encoding
  backend ones. See the protocol chapter of the PostgreSQL manual, "Message
  Formats", for the layouts.
  """

  @ssl_request 80_877_103
  @gss_request 80_877_104
  @cancel_request 80_877_102

  # Anything larger is not a query this store could answer, and refusing it
  # bounds what one connection can make the node buffer.
  @max_message 16 * 1024 * 1024

  # ------------------------------------------------------------------ decoding

  @doc """
  Decodes the first packet of a connection.

  Returns `{:ssl, rest}`, `{:gss, rest}`, `{:cancel, pid, key, rest}`,
  `{:startup, {major, minor}, params, rest}`, `:more`, or `{:error, reason}`.
  """
  def decode_startup(<<len::32, rest::binary>>) when len < 8 or len > 10_000,
    do: {:error, {:bad_startup_length, len, byte_size(rest)}}

  def decode_startup(<<len::32, rest::binary>> = buffer) when byte_size(buffer) >= len do
    body_len = len - 4
    <<body::binary-size(^body_len), rest::binary>> = rest

    case body do
      <<@ssl_request::32>> ->
        {:ssl, rest}

      <<@gss_request::32>> ->
        {:gss, rest}

      <<@cancel_request::32, pid::32, key::32>> ->
        {:cancel, pid, key, rest}

      <<major::16, minor::16, params::binary>> ->
        {:startup, {major, minor}, decode_params(params), rest}
    end
  end

  def decode_startup(_partial), do: :more

  defp decode_params(binary) do
    binary
    |> :binary.split(<<0>>, [:global])
    |> Enum.reject(&(&1 == ""))
    |> Enum.chunk_every(2, 2, :discard)
    |> Map.new(fn [k, v] -> {k, v} end)
  end

  @doc """
  Decodes one frontend message from the front of `buffer`.

  Returns `{:ok, message, rest}`, `:more`, or `{:error, reason}`.
  """
  def decode(<<_type, len::32, _::binary>>) when len < 4 or len > @max_message,
    do: {:error, {:bad_length, len}}

  def decode(<<type, len::32, rest::binary>>) when byte_size(rest) >= len - 4 do
    body_len = len - 4
    <<body::binary-size(^body_len), rest::binary>> = rest

    {:ok, message(type, body), rest}
  rescue
    # A length that framed correctly around a body that does not parse: the
    # client is not speaking this protocol, and there is no resynchronising.
    _ in [MatchError, ArgumentError, FunctionClauseError] -> {:error, :malformed}
  end

  def decode(_partial), do: :more

  defp message(?Q, body), do: {:query, cstring!(body)}
  defp message(?X, _body), do: :terminate
  defp message(?S, _body), do: :sync
  defp message(?H, _body), do: :flush
  defp message(?p, body), do: {:password, cstring!(body)}

  defp message(?P, body) do
    {name, body} = cstring(body)
    {query, <<count::16, oids::binary>>} = cstring(body)
    {:parse, name, query, for(<<oid::32 <- binary_part(oids, 0, count * 4)>>, do: oid)}
  end

  defp message(?B, body) do
    {portal, body} = cstring(body)
    {statement, body} = cstring(body)
    <<nformats::16, body::binary>> = body
    <<formats::binary-size(^nformats * 2), body::binary>> = body
    <<nparams::16, body::binary>> = body
    {params, body} = values(body, nparams, [])
    <<nresults::16, results::binary-size(nresults * 2), _::binary>> = body

    {:bind, portal, statement, for(<<f::16 <- formats>>, do: f), params,
     for(<<f::16 <- results>>, do: f)}
  end

  defp message(?D, <<kind, name::binary>>), do: {:describe, kind(kind), cstring!(name)}
  defp message(?C, <<kind, name::binary>>), do: {:close, kind(kind), cstring!(name)}

  defp message(?E, body) do
    {portal, <<max_rows::32-signed, _::binary>>} = cstring(body)
    {:execute, portal, max_rows}
  end

  # CopyData, CopyDone, CopyFail and FunctionCall: there is nothing to copy into
  # and no functions to call by oid, so say so rather than misread them.
  defp message(type, _body), do: {:unsupported, type}

  defp kind(?S), do: :statement
  defp kind(?P), do: :portal

  defp values(rest, 0, acc), do: {Enum.reverse(acc), rest}
  defp values(<<-1::32-signed, rest::binary>>, n, acc), do: values(rest, n - 1, [nil | acc])

  defp values(<<len::32, value::binary-size(len), rest::binary>>, n, acc),
    do: values(rest, n - 1, [value | acc])

  defp cstring(binary) do
    case :binary.split(binary, <<0>>) do
      [string, rest] -> {string, rest}
      [string] -> {string, <<>>}
    end
  end

  defp cstring!(binary), do: binary |> cstring() |> elem(0)

  # ------------------------------------------------------------------ encoding

  @doc "AuthenticationOk."
  def auth_ok, do: frame(?R, <<0::32>>)

  @doc "AuthenticationCleartextPassword."
  def auth_cleartext, do: frame(?R, <<3::32>>)

  @doc "ParameterStatus."
  def parameter_status(name, value), do: frame(?S, [name, 0, value, 0])

  @doc "BackendKeyData, for CancelRequest."
  def backend_key(pid, key), do: frame(?K, <<pid::32, key::32>>)

  @doc "NegotiateProtocolVersion: the newest minor we speak, and the options we ignore."
  def negotiate_protocol(minor, unsupported) do
    frame(?v, [<<minor::32, length(unsupported)::32>>, Enum.map(unsupported, &[&1, 0])])
  end

  @doc """
  ReadyForQuery: `:idle`, `:transaction` or `:failed`. Nothing is ever rolled
  back, but drivers decide whether to send BEGIN and COMMIT from this, so it is
  kept the way PostgreSQL keeps it.
  """
  def ready(status \\ :idle)
  def ready(:idle), do: frame(?Z, "I")
  def ready(:transaction), do: frame(?Z, "T")
  def ready(:failed), do: frame(?Z, "E")

  @doc "ParseComplete, BindComplete, CloseComplete, NoData, EmptyQueryResponse, PortalSuspended."
  def parse_complete, do: frame(?1, "")
  def bind_complete, do: frame(?2, "")
  def close_complete, do: frame(?3, "")
  def no_data, do: frame(?n, "")
  def empty_query, do: frame(?I, "")
  def portal_suspended, do: frame(?s, "")

  @doc "CommandComplete."
  def command_complete(tag), do: frame(?C, [tag, 0])

  @doc "ParameterDescription."
  def parameter_description(oids),
    do: frame(?t, [<<length(oids)::16>>, Enum.map(oids, &<<&1::32>>)])

  @doc """
  RowDescription. `columns` are `{name, type}` with a type from `Kurwa.Pg.Types`;
  `formats` is one result format per column (0 text, 1 binary).
  """
  def row_description(columns, formats) do
    fields =
      columns
      |> Enum.zip(formats)
      |> Enum.map(fn {{name, type}, format} ->
        {oid, size} = Kurwa.Pg.Types.oid(type)
        [name, 0, <<0::32, 0::16, oid::32, size::16-signed, -1::32-signed, format::16>>]
      end)

    frame(?T, [<<length(columns)::16>>, fields])
  end

  @doc "DataRow. Values are already encoded binaries, or nil for NULL."
  def data_row(values) do
    cells =
      Enum.map(values, fn
        nil -> <<-1::32-signed>>
        value -> [<<byte_size(value)::32>>, value]
      end)

    frame(?D, [<<length(values)::16>>, cells])
  end

  @doc "ErrorResponse."
  def error(code, message, detail \\ nil), do: frame(?E, fields("ERROR", code, message, detail))

  @doc "FATAL ErrorResponse, sent before closing the connection."
  def fatal(code, message), do: frame(?E, fields("FATAL", code, message, nil))

  @doc "NoticeResponse at WARNING."
  def warning(message), do: frame(?N, fields("WARNING", "01000", message, nil))

  defp fields(severity, code, message, detail) do
    [
      [?S, severity, 0],
      [?V, severity, 0],
      [?C, code, 0],
      [?M, message, 0],
      if(detail, do: [?D, detail, 0], else: []),
      0
    ]
  end

  defp frame(type, body) do
    body = IO.iodata_to_binary(body)
    <<type, byte_size(body) + 4::32, body::binary>>
  end
end
