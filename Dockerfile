# Multi-stage build for an AegisFS node image.
# NOTE: Docker is not installed on the original development host, so Docker Compose scenarios are
# documented but marked NOT RUN in FINAL_REPORT.md. On a host with Docker this image builds and
# `compose.yaml` launches a 10-node cluster. The process-local path (scripts/cluster.sh) provides
# the same behavior without Docker and is what has been exercised.
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake ninja-build clang libssl-dev ca-certificates git \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++ \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo -DAEGISFS_BUILD_TESTS=OFF \
 && cmake --build build --target aegisfs

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/aegisfs /usr/local/bin/aegisfs
VOLUME ["/data"]
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/aegisfs"]
CMD ["serve", "--data", "/data", "--host", "0.0.0.0", "--port", "9100"]
