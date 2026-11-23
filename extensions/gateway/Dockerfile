FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake ninja-build clang libssl-dev ca-certificates git python3 \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DAEGISFS_BUILD_TESTS=OFF \
 && cmake --build build --target aegisfs
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 python3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/aegisfs /usr/local/bin/aegisfs
COPY extensions/gateway/aegisfsGateway.py /app/gateway.py
COPY extensions/metrics/prometheusExporter.py /app/prometheusExporter.py
ENTRYPOINT ["python3"]
