#!/usr/bin/env bash
set -euxo pipefail

nix-shell --pure --keep NPM_TOKEN --keep BRANCH_NAME --command "$(cat <<NIXCMD
  set -euxo pipefail
  cd /workspace
  # Change to actual branch
  git branch \$BRANCH_NAME
  git checkout \$BRANCH_NAME
  # Configure git user
  git remote set-url origin git@github.com:malloydata/malloy-mysql-connection.git
  git config --global user.email "malloy-ci-bot@google.com"
  git config --global user.name "Malloy CI Bot"
  # Build
  npm --no-audit --no-fund ci --loglevel error
  npm run lint && npm run build # TODO: run tests here too.
  # Publish and bump version
  echo Publishing and updating version
  # Push new version to github
  npm version patch --force && npm publish ./ --access=public
  git push origin \$BRANCH_NAME
NIXCMD
)"
