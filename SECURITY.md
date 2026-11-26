# Security policy

AegisFS is a personal project and has not been hardened for untrusted networks. Do not expose it to
the public internet.

## What is protected

- **TLS is on by default.** `aegisfs serve` and the client refuse to run without certificates and a
  cluster token unless `--insecure` (or `AEGISFS_INSECURE=1`) is given. Clients verify the server
  certificate against the CA and check that it names the host they dialed. TLS 1.2 or newer is
  required. `--tls-require-client` adds mutual TLS.
- **Every request carries a shared cluster token.** A node answers `UNAUTHORIZED` to a request
  without the right one before it reads the payload. The comparison does not stop at the first
  differing byte. `--token-file` keeps the token out of the process list.
- Frames are bounds-checked: header and payload lengths are capped and malformed input is rejected.
- File names are validated against path traversal, and on disk a file is addressed by a hash of its
  name.

## Known limitations

Cluster:

- One shared token grants every operation on every file, including node-to-node operations. There
  are no users or per-file permissions.
- The TLS handshake runs on the node's accept thread with no timeout. A client that opens a TCP
  connection and sends nothing stops the node from accepting other connections, and this needs no
  credentials. An outbound dial to a peer that accepts TCP but never completes the handshake can
  block the same way.
- Headers of up to 16 MiB are read before the token is checked, and an authenticated request can ask
  the node to allocate up to the 8 GiB payload limit. There is no rate limiting or connection quota.
- `--host` accepts only an IPv4 address. Any other value, such as a host name, binds all interfaces.
- Data on disk is not encrypted.

REST gateway and Compose files:

- The gateway serves plain HTTP, listens on `0.0.0.0` by default and accepts every request when
  `AEGISFS_AUTH_TOKEN` is unset. It reads whole uploads into memory with no size limit.
- `deploy/docker/compose.yaml` publishes the node ports, the gateway (token `change-me`), the
  exporter, Prometheus and Grafana (default login) on all host interfaces. It is for a private
  development machine.
- The development Cassandra service has no TLS or login, and the HDFS test cluster uses `simple`
  authentication over HTTP. Those two publish their own ports on loopback only.

## Local credentials

`scripts/devCreds.sh` writes throwaway certificates and a token to `.dev/`, and
`deploy/docker/cassandra/secureCreds.sh` writes test credentials to
`deploy/docker/cassandra/.dev/`. Both directories are ignored by Git and excluded from Docker build
contexts. The Cassandra keystore and its password are readable by all local users so that the
container can mount them. None of these may be used outside local testing.

The optional integrations apply their own rules, described in
[docs/integrations/overview.md](docs/integrations/overview.md).

## Reporting a vulnerability

For a sensitive problem, open an issue on `sushildalavi/aegisfs` that asks for a private channel and
leave the details out. Non-sensitive hardening suggestions can be described in the issue itself,
with a reproduction and the output of `aegisfs version`.
