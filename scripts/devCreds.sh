#!/usr/bin/env bash
# Throwaway TLS credentials and a cluster token for running AegisFS locally.
#
#   source scripts/devCreds.sh      # exports the AEGISFS_* variables into your shell
#   scripts/devCreds.sh             # prints the export lines instead
#
# Everything lives in .dev/ at the repo root (gitignored) and is created once. The certificate is
# valid for 127.0.0.1 and localhost; set AEGISFS_DEV_SANS="DNS:node0,DNS:node1" to add more names.
# These are for local runs only. Do not use them anywhere real.

# zsh does not set BASH_SOURCE; there $0 names the sourced file.
_devRoot="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
_devDir="${AEGISFS_DEV_DIR:-$_devRoot/.dev}"

if [ ! -f "$_devDir/token" ] || [ ! -f "$_devDir/node.pem" ]; then
  mkdir -p "$_devDir"
  (
    cd "$_devDir"
    sans="IP:127.0.0.1,DNS:localhost${AEGISFS_DEV_SANS:+,$AEGISFS_DEV_SANS}"
    openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=aegisfs-dev-ca" \
      -keyout ca.key -out ca.pem >/dev/null 2>&1
    openssl req -newkey rsa:2048 -nodes -subj "/CN=aegisfs-node" \
      -keyout node.key -out node.csr >/dev/null 2>&1
    printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n' "$sans" > node.ext
    openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 30 \
      -extfile node.ext -out node.pem >/dev/null 2>&1
    rm -f node.csr node.ext ca.srl
    openssl rand -hex 24 > token
    chmod 600 ca.key node.key token
  )
fi

_exports="export AEGISFS_TLS_CA='$_devDir/ca.pem'
export AEGISFS_TLS_CERT='$_devDir/node.pem'
export AEGISFS_TLS_KEY='$_devDir/node.key'
export AEGISFS_TOKEN_FILE='$_devDir/token'"

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "$_exports"
else
  eval "$_exports"
fi
unset _devRoot _devDir _exports
