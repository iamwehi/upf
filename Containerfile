# Production image for UPF. Multi-stage: build a release binary against the
# FoundationDB client library, then ship a slim runtime carrying only the binary
# and libfdb_c. The same image runs any role — the role is chosen at runtime via
# UPF_ROLES, so writer/pusher/janitor are one image with three configs.

# ---- builder ---------------------------------------------------------------
FROM docker.io/library/rust:1-bookworm AS builder

# foundationdb-sys runs bindgen at build time, which needs libclang.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# The FoundationDB C client library, pinned to the server version we run.
COPY --from=docker.io/foundationdb/foundationdb:7.3.78 \
     /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin upf

# ---- runtime ---------------------------------------------------------------
FROM docker.io/library/debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Runtime needs the dynamic client library too (the binary links -lfdb_c).
COPY --from=docker.io/foundationdb/foundationdb:7.3.78 \
     /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
COPY --from=builder /src/target/release/upf /usr/local/bin/upf

ENV UPF_BIND=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/upf"]
