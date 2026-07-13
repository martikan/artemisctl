# ==========================================
# ENTERPRISE SLSA-COMPLIANT DOCKERFILE
# ==========================================
# We rely exclusively on the binary compiled by the SLSA secure runner.

# We use Google's Distroless static image.
# It contains NO shell, NO package manager, and NO utilities.
# It only contains necessary CA certificates, timezone data, and a non-root user.
FROM gcr.io/distroless/static:nonroot

# Standard OCI labels for enterprise container registries
LABEL org.opencontainers.image.title="artemisctl" \
      org.opencontainers.image.description="CLI tool to manage Activemq Artemis brokers" \
      org.opencontainers.image.vendor="github.com/martikan/artemisctl"

# Ensure we operate in the root directory
WORKDIR /

# Copy the securely compiled binary passed in by GitHub Actions.
# The --chown flag guarantees our non-root user has perfect permissions.
COPY --chown=nonroot:nonroot artemisctl /artemisctl

# Explicitly drop privileges.
# UID 65532 is the heavily restricted 'nonroot' user built into Google's Distroless images.
USER 65532:65532

ENTRYPOINT ["/artemisctl"]
