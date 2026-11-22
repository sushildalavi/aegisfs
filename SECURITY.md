# Security Policy

AegisFS is a learning/portfolio distributed file system. It is **not hardened for untrusted
networks** and should not be exposed to the public internet as-is.

## Known limitations (by design, documented)
- **No transport encryption or authentication** between nodes or clients. Run only on a trusted
  network / loopback. Adding TLS + mutual auth is tracked as future work.
- The protocol is bounds-checked (length caps on headers/payloads, validated framing) to avoid
  trivial memory issues from malformed input, but it does not authenticate peers.
- Filenames are validated to prevent path traversal (`../`, absolute paths, control chars); see
  `StorageEngine::valid_filename` and `StorageFixture.PathValidation`.

## Reporting a vulnerability
Open a private report via GitHub Security Advisories on `sushildalavi/aegisfs`, or a regular issue
for non-sensitive hardening suggestions. Please include a reproduction and the affected version
(`aegisfs version`).

## Hardening roadmap
- TLS between nodes and clients, with per-node identities.
- Capability/token-based client authorization.
- Rate limiting and connection quotas on the RPC server.
