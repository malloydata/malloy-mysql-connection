#!/usr/bin/env bash
set -euxo pipefail

export PACKAGES="./"

nix-shell --pure --keep NPM_TOKEN --keep PACKAGES --keep BRANCH_NAME --command "$(cat <<NIXCMD
  set -euxo pipefail
  cd /workspace
  # Change to actual branch
  git branch \$BRANCH_NAME
  git checkout \$BRANCH_NAME
  # Configure git user
  git remote set-url origin git@github.com:malloydata/malloy-mysql-connection 
  git config --global user.email "malloy-ci-bot@google.com"
  git config --global user.name "Malloy CI Bot"
  # Build
  npm --no-audit --no-fund ci --loglevel error
  npm run lint && npm run build # TODO: run tests here too.
  # Publish
  echo Publishing \$PACKAGES
  VERSION=\$(jq -r .version ./lerna.json)
  for package in \$PACKAGES; do
    echo Publishing \$package \$VERSION
    npm publish \$package --access=public
  done
  # Tag current version
  git tag v\$VERSION
  git push origin v\$VERSION
  # Bump version
  npx lerna version patch --yes --no-push --no-git-tag-version
  VERSION=\$(jq -r .version ./lerna.json)
  echo Updating to \$VERSION
  # Push new version to github
  git status
  git commit -am "Version \$VERSION-dev"
  git push origin \$BRANCH_NAME
NIXCMD
)"
