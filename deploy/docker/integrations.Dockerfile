# Image for the optional integrations (Cassandra metadata index, HDFS import/export). It carries the
# aegisfs client binary and the Python package, and nothing the storage nodes depend on.
#   docker build -f deploy/docker/integrations.Dockerfile -t aegisfs-integrations .
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake ninja-build clang libssl-dev ca-certificates git \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++ \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo -DAEGISFS_BUILD_TESTS=OFF \
 && cmake --build build --target aegisfs

FROM ubuntu:24.04 AS integrations
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-venv libssl3 ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY extensions/integrations/requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
COPY --from=build /src/build/aegisfs /usr/local/bin/aegisfs
COPY extensions/integrations /opt/aegisfs-integrations
COPY tests/integrations /opt/aegisfs-integration-tests
ENV PATH="/opt/venv/bin:${PATH}" PYTHONUNBUFFERED=1
WORKDIR /opt/aegisfs-integrations
ENTRYPOINT ["/opt/venv/bin/python", "/opt/aegisfs-integrations/aegisfs-integrations"]
