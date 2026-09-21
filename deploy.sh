#!/bin/bash
# Deploys the service to AWS via serverless framework.
#
# Why this script exists: on Windows, packaging the full node_modules tree
# (especially @aws-sdk and csv-parse) blows past the OS file-handle limit
# and serverless deploy bails with EMFILE: too many open files. Pruning to
# production-only deps trims the working set enough to fit, then we restore
# devDeps so local tooling keeps working.
#
# graceful-fs is preloaded via NODE_OPTIONS so fs.open calls queue instead of
# erroring with EMFILE — pruning alone wasn't enough on Node 24 + sls 3.40.
# It's installed --no-save after the prune so package.json stays clean and
# graceful-fs only exists in node_modules during the deploy window.
#
# Usage:  bash deploy.sh          -> stage "dev", which IS production
#         bash deploy.sh test     -> stage "test", the parallel test stack
#
# The live production stack was first deployed under the Serverless default
# stage "dev" and can't be renamed in place; serverless.yml maps stage -> an
# honest envName (dev -> prod). See the note at the top of serverless.yml.
set -e

STAGE="${1:-dev}"

# Schema first: apply pending src/db/migrate/ files to this stage's database
# (secret shipping/<prod|test>, direct host). The Lambdas run no DDL, so a
# failed migration must stop the deploy here (set -e) rather than ship code
# against a schema it does not match. Migrations are additive, so the code
# still live keeps working in the minutes before the new code lands.
echo "==> Applying database migrations for stage '$STAGE'..."
node tools/migrate.js --stage "$STAGE" --apply

echo "==> Pruning dev dependencies..."
npm prune --production

# Install graceful-fs into an isolated dir. Running `npm install graceful-fs`
# in the project root re-syncs the *entire* package.json (devDeps included)
# regardless of --no-save, undoing the prune above. --prefix gives it its
# own node_modules so the project tree stays minimal.
echo "==> Installing graceful-fs into .deploy-tools/..."
rm -rf .deploy-tools
mkdir -p .deploy-tools
npm install --prefix .deploy-tools graceful-fs --no-save --no-package-lock --no-fund --no-audit

echo "==> Deploying stage '$STAGE' with graceful-fs preloaded..."
GFS_PATH="$(pwd -W 2>/dev/null || pwd)/.deploy-tools/node_modules/graceful-fs"
NODE_OPTIONS="--require $GFS_PATH" serverless deploy --stage "$STAGE"

echo "==> Cleaning up .deploy-tools/..."
rm -rf .deploy-tools

echo "==> Restoring dev dependencies..."
npm install

echo "==> Done."
