#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   BUCKET=<your-bucket> PREFIX="" DISTRIBUTION_ID=<dist> ACCESS_API_URL=<lambda_url> ./deploy_access_pages.sh

if [[ -z "${BUCKET:-}" ]]; then echo "BUCKET is required"; exit 1; fi
PREFIX="${PREFIX:-}"
DIST_ID="${DISTRIBUTION_ID:-}"
API_URL="${ACCESS_API_URL:-}"

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
DEST="s3://$BUCKET$PREFIX/pulse/access/"

echo "#1 Prepare temp dir"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
cp -a "$SRC_DIR"/access.html "$TMP/index.html"
cp -a "$SRC_DIR"/verify.html "$TMP"/
cp -a "$SRC_DIR"/*.py "$TMP"/ 2>/dev/null || true
cp -a "$SRC_DIR"/config.json "$TMP"/

if [[ -n "$API_URL" ]]; then
  echo "#2 Inject ACCESS_API_URL into config.json"
  python3 - "$API_URL" "$TMP/config.json" <<'PY'
import json, sys
url, path = sys.argv[1], sys.argv[2]
with open(path, 'r', encoding='utf-8') as f:
    cfg = json.load(f)
cfg['ACCESS_API_URL'] = url
with open(path, 'w', encoding='utf-8') as f:
    json.dump(cfg, f)
print('updated', path)
PY
fi

echo "#3 Upload pages"
aws s3 cp "$TMP/index.html" "$DEST" --cache-control "no-store" --content-type "text/html"
aws s3 cp "$TMP/verify.html" "$DEST" --cache-control "no-store" --content-type "text/html"
aws s3 cp "$TMP/config.json" "$DEST" --cache-control "no-store" --content-type "application/json"

echo "#4 Invalidate CloudFront (optional)"
if [[ -n "$DIST_ID" ]]; then
  aws cloudfront create-invalidation --distribution-id "$DIST_ID" --paths "/pulse/access/*" >/dev/null
fi

echo "Done -> $DEST"


