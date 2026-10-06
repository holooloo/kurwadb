defmodule Kurwa.Pg.Auth do
  @moduledoc """
  PostgreSQL password authentication: SCRAM-SHA-256, md5 and cleartext.

  PostgreSQL has defaulted to SCRAM-SHA-256 since 14, and every current client
  library speaks it, so it is the default here too (`pg_auth`). md5 is kept
  for clients older than PostgreSQL 10's libpq; cleartext only makes sense
  inside TLS.

  The password is `auth_token`, for any user name - kurwadb has one secret,
  not roles. As PostgreSQL does, the server keeps a SCRAM verifier rather than
  checking the password itself on each attempt: a salt, an iteration count,
  and the StoredKey and ServerKey derived from them (RFC 5802, RFC 7677). The
  verifier is computed once per token and kept in `:persistent_term`, because
  PBKDF2 at 4096 iterations is deliberately slow.

  Over TLS, `SCRAM-SHA-256-PLUS` is offered as well, with `tls-server-end-point`
  channel binding (RFC 5929): the proof then covers a hash of the server's
  certificate, so a man in the middle holding a different certificate cannot
  relay the exchange. That is what libpq picks by default when it can, and what
  `channel_binding=require` insists on. Without TLS there is no channel to bind
  and only plain SCRAM is on offer, as in PostgreSQL.
  """

  @iterations 4096
  @mechanism "SCRAM-SHA-256"
  @plus "SCRAM-SHA-256-PLUS"

  @doc "The mechanisms to offer: PLUS first when there is a TLS channel to bind."
  def mechanisms(nil), do: [@mechanism]
  def mechanisms(_binding), do: [@plus, @mechanism]

  # ------------------------------------------------------------------- SCRAM

  @doc """
  Handles the client-first-message. Returns `{:ok, server_first, exchange}`,
  where `exchange` is what `scram_final/2` needs, or `{:error, reason}`.
  """
  def scram_first(mechanism, client_first, token, binding) do
    with {:ok, gs2_header, bare} <- split_gs2(client_first, mechanism, binding),
         {:ok, client_nonce} <- attribute(bare, "r") do
      %{salt: salt, iterations: iterations} = verifier = verifier(token)
      nonce = client_nonce <> Base.encode64(:crypto.strong_rand_bytes(18))
      server_first = "r=#{nonce},s=#{Base.encode64(salt)},i=#{iterations}"

      {:ok, server_first,
       %{
         gs2_header: gs2_header,
         # what c= must carry: the header, then the binding data if PLUS
         channel: gs2_header <> if(mechanism == @plus, do: binding, else: ""),
         client_first_bare: bare,
         server_first: server_first,
         nonce: nonce,
         verifier: verifier
       }}
    end
  end

  @doc """
  Handles the client-final-message. Returns `{:ok, server_final}` when the
  proof checks out, or `{:error, reason}`.
  """
  def scram_final(client_final, exchange) do
    with [without_proof, "p=" <> proof64] <-
           String.split(client_final, ",p=", parts: 2) |> fix_proof(),
         {:ok, binding} <- attribute(without_proof, "c"),
         true <- binding == Base.encode64(exchange.channel) || {:error, :channel_binding},
         {:ok, nonce} <- attribute(without_proof, "r"),
         true <- nonce == exchange.nonce || {:error, :nonce},
         {:ok, proof} <- Base.decode64(proof64) do
      auth_message =
        Enum.join([exchange.client_first_bare, exchange.server_first, without_proof], ",")

      %{stored_key: stored_key, server_key: server_key} = exchange.verifier

      client_signature = hmac(stored_key, auth_message)
      client_key = :crypto.exor(proof, client_signature)

      if byte_size(proof) == 32 and
           Plug.Crypto.secure_compare(:crypto.hash(:sha256, client_key), stored_key) do
        {:ok, "v=" <> Base.encode64(hmac(server_key, auth_message))}
      else
        {:error, :bad_proof}
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :malformed}
    end
  end

  defp fix_proof([without_proof, proof]), do: [without_proof, "p=" <> proof]
  defp fix_proof(_), do: :malformed

  # "n,,": the client does not bind. "y,,": it could, but believes the server
  # cannot - which, when PLUS was offered, means something stripped it from the
  # offer, and RFC 5802 says to fail. "p=tls-server-end-point,,": it binds.
  defp split_gs2(message, mechanism, binding) do
    case {mechanism, String.split(message, ",", parts: 3)} do
      {@mechanism, ["n", "", bare]} ->
        {:ok, "n,,", bare}

      {@mechanism, ["y", "", bare]} when binding == nil ->
        {:ok, "y,,", bare}

      {@mechanism, ["y", "", _bare]} ->
        {:error, :downgrade}

      {@plus, ["p=tls-server-end-point", "", bare]} when binding != nil ->
        {:ok, "p=tls-server-end-point,,", bare}

      _ ->
        {:error, :malformed}
    end
  end

  @doc """
  `tls-server-end-point` binding data for a DER certificate: its hash, with the
  hash of its signature algorithm - SHA-256 when that is MD5 or SHA-1.
  """
  def end_point(der) do
    # the signature algorithm record's name varies by decoder; its OID is field 1
    {:Certificate, _tbs, algorithm, _signature} = :public_key.pkix_decode_cert(der, :plain)
    :crypto.hash(signature_hash(elem(algorithm, 1)), der)
  end

  @sha384 [{1, 2, 840, 113_549, 1, 1, 12}, {1, 2, 840, 10045, 4, 3, 3}]
  @sha512 [{1, 2, 840, 113_549, 1, 1, 13}, {1, 2, 840, 10045, 4, 3, 4}]

  defp signature_hash(oid) when oid in @sha384, do: :sha384
  defp signature_hash(oid) when oid in @sha512, do: :sha512
  defp signature_hash(_oid), do: :sha256

  defp attribute(message, name) do
    message
    |> String.split(",")
    |> Enum.find_value({:error, {:missing, name}}, fn
      <<^name::binary-size(1), "=", value::binary>> -> {:ok, value}
      _ -> nil
    end)
  end

  @doc "The SCRAM verifier for `token`: computed once, then cached."
  def verifier(token) do
    key = {__MODULE__, :crypto.hash(:sha256, token)}

    case :persistent_term.get(key, nil) do
      nil ->
        salt = :crypto.strong_rand_bytes(16)
        salted = :crypto.pbkdf2_hmac(:sha256, token, salt, @iterations, 32)
        client_key = hmac(salted, "Client Key")

        verifier = %{
          salt: salt,
          iterations: @iterations,
          stored_key: :crypto.hash(:sha256, client_key),
          server_key: hmac(salted, "Server Key")
        }

        :persistent_term.put(key, verifier)
        verifier

      verifier ->
        verifier
    end
  end

  defp hmac(key, data), do: :crypto.mac(:hmac, :sha256, key, data)

  # --------------------------------------------------------------------- md5

  @doc "A fresh 4-byte salt for AuthenticationMD5Password."
  def md5_salt, do: :crypto.strong_rand_bytes(4)

  @doc "Checks a PasswordMessage against md5(md5(password <> user) <> salt)."
  def md5_ok?("md5" <> presented, user, salt, token) do
    inner = md5_hex(token <> user)
    Plug.Crypto.secure_compare(presented, md5_hex(inner <> salt))
  end

  def md5_ok?(_presented, _user, _salt, _token), do: false

  defp md5_hex(data), do: :crypto.hash(:md5, data) |> Base.encode16(case: :lower)
end
