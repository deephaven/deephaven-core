#!/usr/bin/env python3
"""Stub Slack webhook trigger endpoint for the Slack Alert Check workflow.

Captures what slackapi/slack-github-action sends and replies 200 {"ok": true},
as Slack does; v4.0.0 onward parses the response as JSON.

Subcommands: serve, wait, verify.
"""

import glob
import http.server
import json
import os
import re
import socket
import sys
import urllib.parse

HOST = "127.0.0.1"
PORT = 8899
CAPTURE_FILE = "slack-contract-captures.jsonl"
WORKFLOWS = ".github/workflows"
SELF_WORKFLOW = "slack-alert-check-ci.yml"
ACTION = "slackapi/slack-github-action"
USES = re.compile(rf"^\s*-?\s*uses:\s*{re.escape(ACTION)}@(\S+)", re.M)

# The three payload shapes the ten call sites use.
EXPECTED = {
    # nightly-check-ci, nightly-publish-ci, pr-merge-webhook, nightly-docs
    "json-block": {
        "repository": "deephaven/deephaven-core",
        "message": "slack alert contract check",
        "link": "https://github.com/deephaven/deephaven-core",
    },
    # nightly-image-check
    "json-inline": {
        "repository": "deephaven/deephaven-core",
        "message": "slack alert contract check",
        "link": "https://github.com/deephaven/deephaven-core",
    },
    # publish-ci (five sites)
    "yaml-kv": {
        "step_id": "slack-alert-contract",
        "action_url": "https://github.com/deephaven/deephaven-core",
    },
}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length).decode("utf-8")
        case = urllib.parse.parse_qs(
            urllib.parse.urlparse(self.path).query
        ).get("case", ["unknown"])[0]
        with open(CAPTURE_FILE, "a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "case": case,
                        "content_type": self.headers.get("content-type"),
                        "body": body,
                    }
                )
                + "\n"
            )
        payload = b'{"ok":true}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        pass  # keep the workflow log readable


def serve():
    # Start from a clean slate so a re-run cannot pass on stale captures.
    if os.path.exists(CAPTURE_FILE):
        os.remove(CAPTURE_FILE)
    http.server.HTTPServer((HOST, PORT), Handler).serve_forever()


def wait(timeout=30.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=1.0):
                return
        except OSError:
            time.sleep(0.25)
    sys.exit(f"stub did not start listening on {HOST}:{PORT} within {timeout}s")


def verify_pins():
    """Assert this check exercises the same pin as the real call sites.

    Dependabot bumps every reference to an action in one PR, so matching pins
    mean a bump is exercised here. On drift, this check would test a version
    nobody uses.
    """
    pins = {}
    for path in sorted(glob.glob(f"{WORKFLOWS}/*.yml")):
        found = USES.findall(open(path, encoding="utf-8").read())
        if found:
            pins[os.path.basename(path)] = found

    failures = []
    mine = pins.get(SELF_WORKFLOW, [])
    others = {f: p for f, p in pins.items() if f != SELF_WORKFLOW}

    if not mine:
        failures.append(
            f"{SELF_WORKFLOW} no longer uses {ACTION} -- this check covers nothing"
        )
    if not others:
        failures.append(f"no {ACTION} call sites found outside {SELF_WORKFLOW}")
    if len(mine) < len(EXPECTED):
        failures.append(
            f"{SELF_WORKFLOW} has {len(mine)} call site(s) but {len(EXPECTED)} "
            "payload shapes are asserted"
        )

    distinct = {p for found in pins.values() for p in found}
    if len(distinct) > 1:
        detail = "\n".join(f"      {f}: {sorted(set(p))}" for f, p in sorted(pins.items()))
        failures.append(f"{ACTION} pins have drifted:\n{detail}")

    if not failures:
        total = sum(len(p) for p in pins.values())
        print(
            f"  ok  pins: {total} {ACTION} call sites across {len(pins)} workflows, "
            f"all on {distinct.pop()}"
        )
    return failures


def verify():
    failures = verify_pins()
    captured = {}
    if not os.path.exists(CAPTURE_FILE):
        failures.append(
            f"no requests were captured ({CAPTURE_FILE} missing) -- the action "
            "did not send anything"
        )
    else:
        with open(CAPTURE_FILE, encoding="utf-8") as handle:
            for line in handle:
                entry = json.loads(line)
                captured[entry["case"]] = entry

    for case, expected in EXPECTED.items():
        entry = captured.get(case)
        if entry is None:
            failures.append(f"{case}: no request reached the stub")
            continue
        if entry["content_type"] != "application/json":
            failures.append(
                f"{case}: content-type was {entry['content_type']!r}, "
                "expected 'application/json'"
            )
        try:
            actual = json.loads(entry["body"])
        except json.JSONDecodeError as err:
            failures.append(f"{case}: body was not JSON ({err}): {entry['body']!r}")
            continue
        if actual != expected:
            failures.append(
                f"{case}: body mismatch\n    expected {expected}\n    actual   {actual}"
            )
        else:
            print(f"  ok  {case}: {entry['body']}")

    unexpected = set(captured) - set(EXPECTED)
    if unexpected:
        failures.append(f"unexpected cases reached the stub: {sorted(unexpected)}")

    if failures:
        # Leave the captures in place; they are the evidence for debugging.
        print("\nSlack alert contract check FAILED:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        sys.exit(1)
    os.remove(CAPTURE_FILE)
    print(f"\nall {len(EXPECTED)} payload shapes sent as expected")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "serve":
        serve()
    elif command == "wait":
        wait()
    elif command == "verify":
        verify()
    else:
        sys.exit(f"usage: {sys.argv[0]} (serve|wait|verify)")
