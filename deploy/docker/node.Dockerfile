# AegisFS node image. Build from the repo root:
#   docker build -f deploy/docker/node.Dockerfile -t aegisfs .
# compose.yaml in this folder uses it to start a 10-node cluster.
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake ninja-build clang libssl-dev ca-certificates git \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++ \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo -DAEGISFS_BUILD_TESTS=OFF \
 && cmake --build build --target aegisfs

FROM ubuntu:24.04 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/aegisfs /usr/local/bin/aegisfs
VOLUME ["/data"]
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/aegisfs"]
CMD ["serve", "--data", "/data", "--host", "0.0.0.0", "--port", "9100"]

# Same image plus the tools the network-fault test uses to delay, drop and block traffic.
# compose.faults.yaml builds this target; the normal stack does not.
FROM runtime AS faults
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables \
 && rm -rf /var/lib/apt/lists/*
