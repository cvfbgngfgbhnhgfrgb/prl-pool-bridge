#!/usr/bin/env bash
# Download the krig miner (Kryptex's own Pearl/PRL miner, 0% fee) for Linux x64.
set -euo pipefail
VER="1.2.0"
URL="https://github.com/kryptex-miners-org/kryptex-miners/releases/download/krig-1-2-0/krig-miner-${VER}-linux-x64.tar.gz"
cd "$(dirname "$0")"
echo "Downloading krig ${VER} ..."
curl -fL -o krig.tar.gz "$URL"
tar xzf krig.tar.gz
BIN="$(find . -maxdepth 2 -name 'krig-miner*' -type f -perm -u+x | head -n1)"
[ -n "$BIN" ] && [ "$BIN" != "./krig-miner" ] && cp "$BIN" ./krig-miner
chmod +x ./krig-miner
rm -f krig.tar.gz
echo "OK -> ./krig-miner"
./krig-miner --help 2>&1 | head -40 || true
