#!/usr/bin/env bash
set -euo pipefail

TEMPLATE="/opt/keycloak/data/import-template/realm-export.json"
TARGET="/opt/keycloak/data/import/realm-export.json"

if [ -f "$TEMPLATE" ]; then
  if [ -z "${LEDGER_CLI_CLIENT_SECRET:-}" ]; then
    echo "ERROR: LEDGER_CLI_CLIENT_SECRET must be set to render realm-export.json" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$TARGET")"
  while IFS= read -r line || [ -n "$line" ]; do
    printf '%s\n' "${line//\$\{LEDGER_CLI_CLIENT_SECRET\}/${LEDGER_CLI_CLIENT_SECRET}}"
  done < "$TEMPLATE" > "$TARGET"
fi

exec /opt/keycloak/bin/kc.sh "$@"
