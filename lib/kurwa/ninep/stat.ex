defmodule Kurwa.NineP.Stat do
  @moduledoc """
  A 9P stat structure.

  kurwadb fills in what it actually knows and leaves the Unix-flavoured fields at
  sensible constants: there are no real owners, no mtimes worth reporting per key,
  and every key file has length 0 because a key has no contents - its existence
  *is* the data.
  """

  defstruct type: 0,
            dev: 0,
            qid: {0, 0, 0},
            mode: 0o444,
            atime: 0,
            mtime: 0,
            length: 0,
            name: "",
            uid: "kurwa",
            gid: "kurwa",
            muid: "kurwa"

  @type t :: %__MODULE__{}
end
