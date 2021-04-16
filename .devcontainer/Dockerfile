# [Choice] Go version: 1, 1.16, 1.15
ARG VARIANT=1
FROM mcr.microsoft.com/vscode/devcontainers/go:0-${VARIANT}

# [Option] Install Node.js
ARG INSTALL_NODE="true"
ARG NODE_VERSION="lts/*"
RUN if [ "${INSTALL_NODE}" = "true" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi

# Install Docker CLI
ARG USERNAME=automatic
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ADD https://raw.githubusercontent.com/microsoft/vscode-dev-containers/master/containers/docker-from-docker/.devcontainer/library-scripts/docker-debian.sh /tmp/library-scripts/

# Use Docker script from script library to set things up
RUN /bin/bash /tmp/library-scripts/docker-debian.sh "${ENABLE_NONROOT_DOCKER}" "/var/run/docker-host.sock" "/var/run/docker.sock" "${USERNAME}" "${USE_MOBY}"

ENTRYPOINT [ "/usr/local/share/docker-init.sh" ]
CMD [ "sleep", "infinity" ]
