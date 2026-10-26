"""Exercise the image identities used by manual and automatic builds."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github/scripts/image-metadata.sh"
REVISION = subprocess.check_output(
    ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
).strip()
IMAGE = "ghcr.io/mininglamp-oss/octo-im"
BRANCH = "v2.2.5-20260422-fix1"


class ImageMetadataTest(unittest.TestCase):
    def metadata(self, event, ref, tag="", repository="Mininglamp-OSS/octo-im"):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "output"
            result = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env={
                    **os.environ,
                    "GITHUB_EVENT_NAME": event,
                    "GITHUB_REF": ref,
                    "GITHUB_SHA": REVISION,
                    "GITHUB_REPOSITORY": repository,
                    "GITHUB_OUTPUT": str(output),
                    "INPUT_TAG": tag,
                },
                capture_output=True,
                text=True,
            )
            return result, output.read_text() if output.exists() else ""

    def test_branch_push_has_branch_and_commit_tags(self):
        result, output = self.metadata("push", f"refs/heads/{BRANCH}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"{IMAGE}:branch-{BRANCH}\n", output)
        self.assertIn(f"{IMAGE}:sha-{REVISION[:12]}\n", output)
        self.assertIn(f"revision={REVISION}\n", output)
        self.assertNotIn(f"{IMAGE}:latest", output)
        self.assertNotIn(f"{IMAGE}:v2\n", output)

    def test_release_push_preserves_exact_tag(self):
        tag = f"{BRANCH}.2"
        result, output = self.metadata("push", f"refs/tags/{tag}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"version={tag}\n", output)
        self.assertIn(f"{IMAGE}:{tag}\n", output)

    def test_dated_release_push_preserves_exact_tag(self):
        for tag in ("v2.2.5-20261021-fix1", "v2.2.5-20261021-fix1.2", "v2.2.5-20261021-fix1-rc1"):
            with self.subTest(tag=tag):
                result, output = self.metadata("push", f"refs/tags/{tag}")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"version={tag}\n", output)
                self.assertIn(f"{IMAGE}:{tag}\n", output)
                self.assertIn(f"{IMAGE}:sha-{REVISION[:12]}\n", output)

    def test_dispatch_defaults_to_source_specific_test_tag(self):
        result, output = self.metadata("workflow_dispatch", "refs/heads/feature/build")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"{IMAGE}:test-{REVISION[:12]}\n", output)

    def test_dispatch_accepts_explicit_test_tag(self):
        result, output = self.metadata(
            "workflow_dispatch", f"refs/heads/{BRANCH}", "test-fix1-candidate"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"{IMAGE}:test-fix1-candidate\n", output)

    def test_dispatch_rejects_reserved_invalid_or_unscoped_tags(self):
        for tag in ("latest", "v2", BRANCH, "test-", "test-bad/tag", "test-bad\nother", "test-" + "a" * 124):
            with self.subTest(tag=tag):
                result, output = self.metadata("workflow_dispatch", f"refs/heads/{BRANCH}", tag)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("::error::", result.stderr)
                self.assertEqual(output, "")

    def test_automatic_publish_rejects_unrelated_refs_and_events(self):
        for event, ref in (
            ("push", "refs/heads/main"),
            ("push", "refs/tags/v2.2.6"),
            ("push", f"refs/tags/{BRANCH}bad"),
            ("push", "refs/tags/v2.2.6-20261021-fix1"),
            ("push", "refs/tags/v2.2.5-abcdefgh-fix1"),
            ("push", "refs/tags/v2.2.5-2026102-fix1"),
            ("push", "refs/tags/v2.2.5-202610211-fix1"),
            ("push", "refs/tags/v2.2.5-20261021-fix2"),
            ("push", "refs/tags/v2.2.5-20261021-fix1bad"),
            ("push", "refs/tags/v2.2.5-20261021-fix1."),
            ("pull_request", "refs/pull/1/merge"),
        ):
            with self.subTest(event=event, ref=ref):
                result, output = self.metadata(event, ref)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("::error::", result.stderr)
                self.assertEqual(output, "")

    def test_image_namespace_follows_repository(self):
        result, output = self.metadata(
            "workflow_dispatch", "refs/heads/feature", repository="Example-Org/Fork"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("image=ghcr.io/example-org/fork\n", output)


if __name__ == "__main__":
    unittest.main()
