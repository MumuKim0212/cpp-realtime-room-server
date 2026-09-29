# Ubuntu 24.04 carries CMake 3.28 and GCC 13, which clears what the build asks
# for (CMake 3.20+, GCC 13+) without adding a third-party toolchain repository.
FROM ubuntu:24.04 AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        libsodium-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Only what the server target needs. tests/ and tools/ are left out on purpose:
# building the tests fetches GoogleTest over the network, and nothing in a
# deployed container runs them or points a load generator at itself. The two
# OFF switches are what make those directories unnecessary.
COPY CMakeLists.txt ./
COPY src/ src/

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
        -DRRS_BUILD_TESTS=OFF -DRRS_BUILD_LOADTEST=OFF \
    && cmake --build build -j "$(nproc)"

FROM ubuntu:24.04

# The runtime libraries the binary links, without the headers and static
# archives that only the build stage needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsodium23 \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/build/src/room_server /usr/local/bin/room_server

# Nothing the server does needs root, and the port it binds is above 1024.
USER ubuntu

EXPOSE 9000

# Exec form, so the server is PID 1 and receives SIGTERM directly. It blocks
# that signal and reads it back through a descriptor, which is how `docker
# stop` gets a clean shutdown rather than a ten-second wait and a kill.
ENTRYPOINT ["/usr/local/bin/room_server"]
