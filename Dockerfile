# Shopping Sessions: native Rust pipeline.
FROM rust:1.97-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# the scorers are Intel Xeon (Cascade Lake or newer) with AVX-512: build for x86-64-v4
ENV RUSTFLAGS="-C target-cpu=x86-64-v4"
RUN cargo build --release --locked && cp target/release/shopping-sessions /shopping-sessions

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
