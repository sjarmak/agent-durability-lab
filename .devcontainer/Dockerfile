# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

FROM ghcr.io/astral-sh/uv:0.11.2@sha256:c4f5de312ee66d46810635ffc5df34a1973ba753e7241ce3a08ef979ddd7bea5 AS uv
FROM golang:1.25.12-bookworm@sha256:6359592445455f2dbe2412bed411336035bc019a50017720d77454ffdd6d0f82

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000

COPY --from=uv --chmod=0755 /uv /uvx /usr/local/bin/

RUN groupadd --gid "${USER_GID}" "${USERNAME}" \
    && useradd --uid "${USER_UID}" --gid "${USER_GID}" --create-home --shell /bin/bash "${USERNAME}" \
    && install -d -o "${USER_UID}" -g "${USER_GID}" \
      /home/vscode/.cache \
      /home/vscode/.cache/go-build \
      /home/vscode/.cache/uv \
      /home/vscode/.local \
      /home/vscode/.local/bin \
      /home/vscode/.local/share \
      /home/vscode/.local/share/uv \
      /home/vscode/.local/share/uv/python \
      /home/vscode/go \
      /home/vscode/go/pkg \
      /home/vscode/go/pkg/mod \
      /workspaces/temporal_projects

ENV GOCACHE=/home/vscode/.cache/go-build \
    GOPATH=/home/vscode/go \
    GOMODCACHE=/home/vscode/go/pkg/mod \
    UV_NO_PROGRESS=1 \
    UV_PYTHON_BIN_DIR=/home/vscode/.local/bin \
    UV_PYTHON_INSTALL_DIR=/home/vscode/.local/share/uv/python \
    UV_PYTHON_PREFERENCE=only-managed \
    PATH=/home/vscode/.local/bin:/usr/local/go/bin:/home/vscode/go/bin:/usr/local/bin:/usr/bin:/bin

USER vscode
RUN uv python install 3.12.12

WORKDIR /workspaces/temporal_projects
CMD ["sleep", "infinity"]
