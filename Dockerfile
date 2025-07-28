# Stage 1: Build
FROM rust:latest AS builder

WORKDIR /app
COPY Cargo.toml .
# Only copy Cargo.lock if it exists
COPY Cargo.lock . 
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release

COPY src/ src/
RUN touch src/main.rs && \
    cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim  
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/stavros_project /usr/local/bin/
CMD ["stavros_project"]