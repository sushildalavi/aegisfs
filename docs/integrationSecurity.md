# Integration security

This covers the two optional integrations. The security of the cluster itself (TLS between nodes and
clients, the shared cluster token) is described in [SECURITY.md](../SECURITY.md) and is unchanged.

## Rules both integrations follow

- **Secure by default.** Each integration refuses an unprotected connection unless a development
  switch is set by name. Nothing quietly falls back to plaintext, and nothing turns certificate
  checking off as a side effect of another setting.
- **Secrets stay out of configuration.** Passwords and tokens are read from files named in the
  configuration (`password_file`, `token_file`), never from flags or inline JSON, and never from the
  source. Error messages and logs are passed through a redaction step, and tests check that a wrong
  password does not appear in library errors, in the CLI's output or in its JSON output.
- **The cluster path is not bypassed.** Both integrations reach AegisFS by running the `aegisfs`
  client, so TLS and the cluster token apply to every request they make, with the same environment
  variables as any other client.
- **Local files are private.** The Cassandra retry spool is created owner-only (mode 0600) and the
  HDFS journal directory is owner-only (0700). The spool holds file names and operation types; the
  journal holds names and hashes. Neither holds file contents or credentials.
- **Nothing is deleted from either side.** A transfer never deletes the AegisFS file or the HDFS
  file. The only delete the HDFS connector can issue is for its own temporary names.

## Cassandra

| Setting | Behaviour |
| --- | --- |
| TLS | On by default, with certificate and host name verification. A CA file can be given. |
| Plaintext | Needs `allow_insecure=true`. Without it, `tls=false` is a configuration error. |
| Skipping verification | `tls_verify=false` needs the same `allow_insecure` switch, and the connection stays encrypted but unverified. |
| Login | `username` plus `password_file`. A username without a password file is a configuration error. |

A bad login or an untrusted certificate is reported as a configuration problem: the command exits
with code 3 and queues nothing, because retrying cannot fix it. Only a genuine outage (nothing
answering) sends index work to the local queue.

What was tested against a real Cassandra started from the Compose file in secure mode (TLS required,
`PasswordAuthenticator`): the right user, password file and CA connect; a wrong password is
refused and does not appear in any output; a wrong CA, and no CA at all, fail verification; TLS
required against the plaintext container fails instead of falling back; plaintext against the
secure server is refused; and `migrate`, indexing and a confirmed lookup work over the secure
connection.

Not tested: client certificates (mutual TLS) to Cassandra, authorization (the `aegis_index` login
is not permission-restricted in these tests), several data centres, and a Cassandra that was not
started from this Compose file.

The development container generates its certificates and random passwords into a gitignored
directory the first time it is started. They are throwaway and for local testing only.

## HDFS

| Setting | Behaviour |
| --- | --- |
| `https://` NameNode | The certificate and host name are verified, against the system CAs or `ca_file`. There is no option to turn verification off. |
| `http://` NameNode | Refused unless `allow_insecure_http` is set. Every command then prints a warning. For development clusters only. |
| `auth=simple` | Sends `user.name`. HDFS accepts this without proof, so it is only as strong as the cluster's own setting. |
| `auth=token` | Sends a delegation token read from `token_file`. Tested only against an in-process fake server, because the test cluster has no Kerberos and cannot issue tokens. |
| Kerberos | **Not supported.** |

### Redirects

WebHDFS answers a write or read request to the NameNode with a redirect to a DataNode, and the
data (and sometimes credentials) go to whatever address the redirect names. A hostile or
misconfigured NameNode could use that to send data somewhere else, so the connector never lets the
HTTP library follow a redirect on its own. It checks every hop:

- the scheme must match the NameNode URL, so there is no downgrade from HTTPS to HTTP;
- the host must be the NameNode or appear in `allowed_redirect_hosts`;
- the URL must not contain a user name or password, control characters or an invalid port;
- at most three redirects are followed, and a loop is refused;
- the user name or token is added only to the request sent to the NameNode, and a redirect target is
  requested exactly as written, with nothing added;
- the file body is sent only to the final, validated address.

The unit tests refuse a redirect to an unlisted host, a scheme downgrade, a loop and a redirect with
embedded credentials, and a live test shows a redirect to an unlisted host is refused with nothing
sent.

### Names and paths

Names are checked before any request is made. `.` and `..` parts, empty parts, control characters
and, for HDFS paths, `:` and backslash are rejected, and an AegisFS name may not start with `/` or
contain the `.stripe.` text reserved for striped chunks. HDFS paths must be absolute. When `root` is
configured, a relative path is taken to be below it and an absolute path outside it is refused.

### What is not covered

HDFS federation, high-availability failover (the connector does not retry against a second
NameNode), encryption zones and erasure coding were not tested. HTTPS to HDFS was tested only
against an in-process fake server.

## Reporting a problem

Security issues should be reported as described in [SECURITY.md](../SECURITY.md).
