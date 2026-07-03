#!/bin/sh
# Points git at the version-controlled hooks in this directory.
set -e
cd "$(git rev-parse --show-toplevel)"
git config core.hooksPath .githooks
chmod +x .githooks/pre-commit
echo "Installed: git will run .githooks/pre-commit before each commit."
