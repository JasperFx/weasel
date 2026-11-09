#!/usr/bin/env bash
# Pin Weasel to a locally packed JasperFx and register the local feed.
#   scripts/take-local-upstream.sh <jasperfx-version> <feed-folder>
# Pass the released version (and no feed) afterwards to put the pins back.
set -euo pipefail
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <jasperfx-version> [feed-folder]" >&2
  exit 2
fi
cd "$(dirname "${BASH_SOURCE[0]}")/.."
args=(TakeLocalUpstream --upstream-version "$1")
if [[ -n "${2:-}" ]]; then args+=(--feed "$2"); fi
./build.sh "${args[@]}"
