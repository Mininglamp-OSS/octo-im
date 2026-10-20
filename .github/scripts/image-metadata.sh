#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "::error::$*" >&2
  exit 1
}

revision="$(git rev-parse HEAD)"
[[ "$revision" == "$GITHUB_SHA" ]] || fail 'Checkout does not match the triggered commit.'
short_sha="${revision:0:12}"
branch='v2.2.5-20260422-fix1'

case "$GITHUB_EVENT_NAME" in
  workflow_dispatch)
    version="${INPUT_TAG:-test-${short_sha}}"
    [[ "$version" == test-?* ]] || fail 'Manual image tags must start with test- and include a suffix.'
    ;;
  push)
    case "$GITHUB_REF" in
      "refs/heads/$branch") version="branch-${branch}" ;;
      "refs/tags/$branch"|"refs/tags/$branch"[.-]*) version="${GITHUB_REF#refs/tags/}" ;;
      *) fail "Automatic publishing is restricted to $branch and its release tags." ;;
    esac
    ;;
  *) fail "Unsupported event: $GITHUB_EVENT_NAME" ;;
esac

[[ "$version" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || fail 'Invalid Docker image tag.'
image="ghcr.io/$(printf '%s' "$GITHUB_REPOSITORY" | tr '[:upper:]' '[:lower:]')"
{
  printf 'image=%s\nversion=%s\nrevision=%s\n' "$image" "$version" "$revision"
  printf 'commit_date=%s\n' "$(git log -1 --format=%ct)"
  printf 'tags<<IMAGE_TAGS\n%s:%s\n%s:sha-%s\nIMAGE_TAGS\n' "$image" "$version" "$image" "$short_sha"
} >> "$GITHUB_OUTPUT"
