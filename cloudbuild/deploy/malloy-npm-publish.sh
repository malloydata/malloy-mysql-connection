#!/usr/bin/env bash
set -euxo pipefail

export PACKAGES="./"

nix-shell --pure --keep NPM_TOKEN --keep PACKAGES --command "$(cat <<NIXCMD
  set -euxo pipefail
  cd /workspace
  git branch -m main
  npm --no-audit --no-fund ci --loglevel error
  echo Publishing \$PACKAGES
  PRERELEASE=\$(date +%y%m%d%H%M%S)
  for package in \$PACKAGES; do
    echo Publishing \$package \$VERSION
    npm publish -w \$package --access=public --tag next
  done
NIXCMD
)"
