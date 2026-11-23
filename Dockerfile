# Multi-stage build for an AegisFS node image, used by compose.yaml to launch a 10-node cluster.
# scripts/cluster.sh provides the same process-local behavior without Docker.
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
