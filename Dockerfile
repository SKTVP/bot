# Use the official Rust image
FROM rust:latest as builder

# Copy only necessary files (Cargo.toml + src/main.rs)
WORKDIR /app
COPY Cargo.toml .
COPY src/main.rs src/

# Build the release binary
RUN cargo build --release

# Use a minimal runtime image
FROM debian:buster-slim
COPY --from=builder /app/target/release/stavros_project /usr/local/bin/

# Run the binary
CMD ["stavros_project"]