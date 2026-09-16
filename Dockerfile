# Build the wheel separately so the runtime image carries no build toolchain.
FROM python:3.13-slim AS builder

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir build==1.2.2 \
    && python -m build --wheel --outdir /dist

FROM python:3.13-slim

# keyring has no Secret Service here on purpose: credentials arrive as
# ZEPP_APP_TOKEN / ZEPP_APP_TOKEN_FILE from a Kubernetes Secret, and the code
# treats an unavailable keyring as a normal condition rather than an error.
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    XDG_DATA_HOME=/data \
    XDG_CONFIG_HOME=/data/config \
    ZEPP_MCP_TRANSPORT=http \
    ZEPP_MCP_HOST=0.0.0.0 \
    ZEPP_MCP_PORT=8080

COPY --from=builder /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl \
    && rm -f /tmp/*.whl \
    && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin zepp \
    && mkdir -p /data \
    && chown 10001:10001 /data

USER 10001
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080

ENTRYPOINT ["zepp-life-mcp"]
CMD ["serve"]
