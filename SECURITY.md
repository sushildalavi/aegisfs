# Security Policy

AegisFS is a personal project and has not been hardened for untrusted networks. Do not expose it
to the public internet as it is.

## Known limitations
- **Transport security is optional and must be enabled explicitly.** Without TLS, nodes and binary
  clients must run on a trusted network. TLS + mutual authentication is available through the
  `serve` and client TLS flags; the REST gateway additionally supports bearer-token authentication.
- The protocol is bounds-checked (length caps on headers/payloads, validated framing) to avoid
  trivial memory issues from malformed input. Peer authentication requires TLS/mTLS.
- Filenames are validated to prevent path traversal (`../`, absolute paths, control chars); see
  `StorageEngine::validFilename` and `StorageFixture.PathValidation`.

## Reporting a vulnerability
Open a private report via GitHub Security Advisories on `sushildalavi/aegisfs`, or a regular issue
for non-sensitive hardening suggestions. Please include a reproduction and the affected version
(`aegisfs version`).

## Hardening roadmap
- Enforce TLS between nodes and clients, with per-node identities, in every deployment.
- Add capability/token-based authorization to the binary RPC protocol; bearer authentication
  currently applies to the REST gateway.
- Rate limiting and connection quotas on the RPC server.
