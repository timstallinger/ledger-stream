#!/bin/sh
set -eu

API_BASE_URL="${VITE_API_BASE_URL:-http://localhost:8080}"
OIDC_AUTHORITY="${VITE_OIDC_AUTHORITY:-http://localhost:8180/realms/ledger-stream}"
OIDC_CLIENT_ID="${VITE_OIDC_CLIENT_ID:-ledger-stream-spa}"

cat <<EOF > /tmp/config.js
window.__LEDGER_CONFIG__ = {
  apiBaseUrl: '${API_BASE_URL}',
  oidcAuthority: '${OIDC_AUTHORITY}',
  oidcClientId: '${OIDC_CLIENT_ID}'
};
EOF
