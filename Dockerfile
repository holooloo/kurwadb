# kurwadb in a container: one node, every frontend on, data on a volume.
#
#     docker build --target runtime -t kurwadb .   the image to run
#     docker build --target test -t kurwadb-test . the same build, with the
#                                                  suite run against both engines
#
# The stages are ordered so the classic builder (no BuildKit) gets the same
# result: it builds every stage up to the target, and `runtime` comes before
# `test`.

ARG ELIXIR_IMAGE=elixir:1.20-otp-29
ARG RUNTIME_IMAGE=debian:trixie-slim

# ------------------------------------------------------------------ base
# Elixir plus cargo: Kurwa.Native is compiled only when cargo is on the path,
# and without it the image would quietly fall back to the Elixir Bloom filter.
FROM ${ELIXIR_IMAGE} AS base

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates build-essential \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable

ENV PATH=/root/.cargo/bin:$PATH \
    LANG=C.UTF-8

WORKDIR /src
RUN mix local.hex --force && mix local.rebar --force

COPY mix.exs mix.lock ./
RUN mix deps.get

COPY config config
COPY native native
COPY lib lib
COPY test test
COPY examples examples
COPY priv/dashboard priv/dashboard

# --------------------------------------------------------------- release
FROM base AS release
ENV MIX_ENV=prod
RUN mix deps.compile && mix compile && mix release --path /opt/kurwadb

# --------------------------------------------------------------- runtime
FROM ${RUNTIME_IMAGE} AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends libssl3t64 libncurses6 libstdc++6 libsctp1 ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=release /opt/kurwadb /opt/kurwadb
COPY examples/procedures /procedures

# One node, so a quorum is that node. Ports are the protocols' own; pick the
# host side when publishing.
ENV LANG=C.UTF-8 \
    RELEASE_NODE=kurwadb \
    KURWA_N=1 KURWA_R=1 KURWA_W=1 \
    KURWA_ENGINE=lsm \
    KURWA_DATA_DIR=/data \
    KURWA_PROCEDURES_DIR=/procedures \
    KURWA_HTTP_PORT=4040 \
    KURWA_PG=1 KURWA_PG_PORT=5432 \
    KURWA_MYSQL=1 KURWA_MYSQL_PORT=3306 \
    KURWA_RESP=1 KURWA_RESP_PORT=6379 \
    KURWA_MONGO=1 KURWA_MONGO_PORT=27017 \
    KURWA_MSSQL=1 KURWA_MSSQL_PORT=1433

VOLUME /data
EXPOSE 4040 5432 3306 6379 27017 1433

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s \
  CMD curl -fsS http://127.0.0.1:4040/health || exit 1

CMD ["/opt/kurwadb/bin/kurwadb", "start"]

# ------------------------------------------------------------------ test
# The suite, both engines. Building this stage is running the tests: a
# failure fails the build. The client tests (psql, sqlcmd, drivers) need
# those clients and run against a live container instead - see deploy/.
FROM base AS test
ENV MIX_ENV=test
RUN mix deps.compile && mix compile \
 && mix run -e 'true = Kurwa.Native.available?()' --no-start \
 && mix test \
 && KURWA_TEST_ENGINE=lsm mix test
