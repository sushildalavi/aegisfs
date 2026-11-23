# Security Policy

AegisFS is a personal project and has not been hardened for untrusted networks. Do not expose it
to the public internet as it is.

## What is protected
- **TLS is on by default.** `aegisfs serve` refuses to start, and the client refuses to run,
  without `--tls-cert/--tls-key/--tls-ca` (nodes) or `--tls-ca` (clients) and a cluster token.
  `--insecure` (or `AEGISFS_INSECURE=1`) turns both off for throwaway local use and prints a
  warning. Clients verify the server certificate against the CA and check that it names the host
  they dialed. `--tls-require-client` adds mutual TLS.
- **Every request carries a shared cluster token**, and a node answers `UNAUTHORIZED` to anything
  without the right one before it reads the payload. The token is compared in constant time and is
  read from a file (`--token-file`) so it stays out of the process list. It is a single shared secret
  for the whole cluster, not a per-user identity.
- The protocol is bounds-checked (length caps on headers and payloads, validated framing).
- Filenames are validated to prevent path traversal (`../`, absolute paths, control characters).

## Known limitations
- One shared token means any holder can do anything, including reading and deleting every file.
  There are no per-file permissions or per-user identities.
- Data on disk is not encrypted.
- `scripts/devCreds.sh` makes throwaway certificates for local runs. Do not reuse them anywhere real.
- There is no rate limiting or connection quota on the RPC server.

## Reporting a vulnerability
Open a private report via GitHub Security Advisories on `sushildalavi/aegisfs`, or a regular issue
for non-sensitive hardening suggestions. Please include a reproduction and the affected version
(`aegisfs version`).

## Hardening ideas
- Per-user tokens or certificates with per-file permissions.
- Encryption at rest.
- Rate limiting and connection quotas on the RPC server.
