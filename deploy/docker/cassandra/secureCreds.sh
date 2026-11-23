#!/usr/bin/env bash
# Throwaway TLS credentials and passwords for the secure Cassandra test variant.
#
#   deploy/docker/cassandra/secureCreds.sh           # create them if missing, print the directory
#
# Everything is written to deploy/docker/cassandra/.dev/ (override with AEGIS_CASS_SECURE_DIR),
# which Git ignores, and is created once with random values. Nothing here is a real secret and
# nothing here may be used outside local testing.
#   ca.pem / ca.key      throwaway certificate authority
#   server.p12           server certificate and key for Cassandra (PKCS12), password in keystore.pass
#   wrongCa.pem          an unrelated CA, for testing that a wrong CA is rejected
#   index.pass           password of the login the index uses (role aegis_index)
#   admin.pass           password the built-in superuser is changed to after first start
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR="${AEGIS_CASS_SECURE_DIR:-$HERE/.dev}"

if [ ! -f "$DIR/server.p12" ] || [ ! -f "$DIR/index.pass" ]; then
  mkdir -p "$DIR"
  (
    cd "$DIR"
    umask 077
    # Python 3.13+ with OpenSSL 3 verifies strictly: a CA needs basicConstraints and keyUsage, and
    # the server certificate needs its own usage extensions, key identifiers (the CA's subject key
    # identifier and the server's authority key identifier) and a matching subjectAltName.
    openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=aegis-cassandra-dev-ca" \
      -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
      -addext "subjectKeyIdentifier=hash" -keyout ca.key -out ca.pem >/dev/null 2>&1
    openssl req -newkey rsa:2048 -nodes -subj "/CN=cassandra-secure" -keyout server.key \
      -out server.csr >/dev/null 2>&1
    printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid\nsubjectAltName=IP:127.0.0.1,DNS:localhost,DNS:cassandra-secure\n' > server.ext
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 30 \
      -extfile server.ext -out server.pem >/dev/null 2>&1
    openssl rand -hex 16 > keystore.pass
    openssl pkcs12 -export -in server.pem -inkey server.key -certfile ca.pem -name cassandra \
      -out server.p12 -passout "file:keystore.pass" >/dev/null 2>&1
    openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=some-other-ca" \
      -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
      -keyout wrongCa.key -out wrongCa.pem >/dev/null 2>&1
    openssl rand -hex 16 > index.pass
    openssl rand -hex 16 > admin.pass
    rm -f server.csr server.ext ca.srl wrongCa.key
    # The container's cassandra user has to read what it mounts. These are throwaway values.
    chmod 644 server.p12 keystore.pass ca.pem
    chmod 600 ca.key server.key index.pass admin.pass
  )
fi
echo "$DIR"
