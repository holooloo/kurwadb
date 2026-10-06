defmodule Kurwa.Mysql.Auth do
  @moduledoc """
  MySQL authentication: `caching_sha2_password`, MySQL 8's default, and
  `mysql_native_password` for older clients.

  Both are challenge-response over a 20-byte nonce, so the password - which
  is `auth_token`, for any user - never crosses the wire. caching_sha2's
  "fast path" needs only that the server can compute the expected scramble,
  and it always can here, so it never falls back to the full exchange that
  would want TLS or an RSA key.
  """

  @doc "A 20-byte nonce of printable characters, as MySQL sends."
  def nonce do
    for <<b <- :crypto.strong_rand_bytes(20)>>, into: <<>>, do: <<33 + rem(b, 94)>>
  end

  @doc "Does `response` prove knowledge of `password` for this nonce, under `plugin`?"
  def valid?("caching_sha2_password", response, nonce, password),
    do: caching_sha2(response, nonce, password)

  def valid?("mysql_native_password", response, nonce, password),
    do: native(response, nonce, password)

  def valid?(_plugin, _response, _nonce, _password), do: false

  # XOR(SHA256(pw), SHA256(SHA256(SHA256(pw)) || nonce))
  defp caching_sha2("", _nonce, password), do: password == ""

  defp caching_sha2(response, nonce, password) do
    d1 = :crypto.hash(:sha256, password)
    d2 = :crypto.hash(:sha256, d1)

    byte_size(response) == 32 and
      Plug.Crypto.secure_compare(response, :crypto.exor(d1, :crypto.hash(:sha256, d2 <> nonce)))
  end

  # XOR(SHA1(pw), SHA1(nonce || SHA1(SHA1(pw))))
  defp native("", _nonce, password), do: password == ""

  defp native(response, nonce, password) do
    s1 = :crypto.hash(:sha, password)
    s2 = :crypto.hash(:sha, s1)

    byte_size(response) == 20 and
      Plug.Crypto.secure_compare(response, :crypto.exor(s1, :crypto.hash(:sha, nonce <> s2)))
  end
end
