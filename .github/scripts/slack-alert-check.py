#!/usr/bin/env python3
"""Stub Slack webhook trigger endpoint for the Slack Alert Check workflow.

Captures what slackapi/slack-github-action sends and replies 200 {"ok": true},
as Slack does; v4.0.0 onward parses the response as JSON.

The payloads under test are the real ones, read out of the call sites, so a
malformed payload in any workflow fails this check. Only stdlib is used, so the
check cannot break on a missing package.

Subcommands: sites, pins, serve, wait, verify <case>.
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
CAPTURE_FILE = "slack-alert-captures.jsonl"
WORKFLOWS = ".github/workflows"
SELF_WORKFLOW = "slack-alert-check-ci.yml"
ACTION = "slackapi/slack-github-action"
USES = re.compile(rf"^\s*-?\s*uses:\s*{re.escape(ACTION)}@(\S+)")
ITEM = re.compile(r"^\s*- ")
PAYLOAD = re.compile(r"^(\s*)payload:(.*)$")
EXPR = re.compile(r"\$\{\{(.+?)\}\}", re.S)


def substitute(text):
    """Stand in for GitHub expression evaluation with a single-line placeholder.

    The real values are single-line strings; what matters here is that the
    payload still parses once something has been spliced into it.
    """
    return EXPR.sub(lambda m: f"<{m.group(1).strip()}>", text)


def unquote(value):
    if len(value) > 1 and value[0] == value[-1] == '"':
        return value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    if len(value) > 1 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def read_payload(lines, start, stop):
    """Return the `payload:` input of the step spanning lines[start:stop]."""
    for i in range(start, stop):
        match = PAYLOAD.match(lines[i])
        if not match:
            continue
        indent, rest = len(match.group(1)), match.group(2).strip()
        if rest.startswith("|") or rest.startswith(">"):
            block = []
            for line in lines[i + 1 :]:
                if line.strip() and len(line) - len(line.lstrip()) <= indent:
                    break
                block.append(line)
            while block and not block[-1].strip():
                block.pop()
            pad = min(
                (len(b) - len(b.lstrip()) for b in block if b.strip()), default=0
            )
            return "\n".join(b[pad:] for b in block) + "\n"
        return unquote(rest)
    return None


def call_sites():
    """Every real call site, with its payload as the action will receive it."""
    sites = []
    for path in sorted(glob.glob(f"{WORKFLOWS}/*.yml")):
        name = os.path.basename(path)
        if name == SELF_WORKFLOW:
            continue
        lines = open(path, encoding="utf-8").read().split("\n")
        seen = 0
        for i, line in enumerate(lines):
            if not USES.match(line):
                continue
            stop = next(
                (j for j in range(i + 1, len(lines)) if ITEM.match(lines[j])),
                len(lines),
            )
            payload = read_payload(lines, i, stop)
            seen += 1
            sites.append(
                {
                    "name": f"{name[:-4]}-{seen}",
                    "file": name,
                    "payload": substitute(payload) if payload is not None else None,
                }
            )
    return sites


def sites():
    found = call_sites()
    missing = [s["name"] for s in found if s["payload"] is None]
    if not found:
        sys.exit(f"no {ACTION} call sites found under {WORKFLOWS}")
    if missing:
        sys.exit(f"could not read the payload input of: {missing}")
    print(json.dumps(found, separators=(",", ":")))


def pins():
    """Assert this check exercises the same pin as the real call sites.

    Dependabot bumps every reference to an action in one PR, so matching pins
    mean a bump is exercised here. On drift, this check would test a version
    nobody uses.
    """
    found = {}
    for path in sorted(glob.glob(f"{WORKFLOWS}/*.yml")):
        matched = [
            m.group(1) for m in (USES.match(l) for l in open(path, encoding="utf-8")) if m
        ]
        if matched:
            found[os.path.basename(path)] = matched

    failures = []
    if not found.get(SELF_WORKFLOW):
        failures.append(
            f"{SELF_WORKFLOW} no longer uses {ACTION} -- this check covers nothing"
        )
    if not {f: p for f, p in found.items() if f != SELF_WORKFLOW}:
        failures.append(f"no {ACTION} call sites found outside {SELF_WORKFLOW}")

    distinct = {p for matched in found.values() for p in matched}
    if len(distinct) > 1:
        detail = "\n".join(f"      {f}: {sorted(set(p))}" for f, p in sorted(found.items()))
        failures.append(f"{ACTION} pins have drifted:\n{detail}")

    if failures:
        print("Slack alert check FAILED:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        sys.exit(1)
    total = sum(len(p) for p in found.values())
    print(f"  ok  {total} {ACTION} call sites across {len(found)} workflows, all on {distinct.pop()}")


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


def verify(case):
    """Assert the one request for `case` arrived and is something Slack accepts."""
    failures = []
    entry = None
    rest = []
    if os.path.exists(CAPTURE_FILE):
        for line in open(CAPTURE_FILE, encoding="utf-8"):
            record = json.loads(line)
            if record["case"] == case:
                entry = record
            else:
                rest.append(line)

    if entry is None:
        failures.append(f"no request reached the stub for {case}")
    else:
        if entry["content_type"] != "application/json":
            failures.append(
                f"content-type was {entry['content_type']!r}, expected 'application/json'"
            )
        try:
            body = json.loads(entry["body"])
        except json.JSONDecodeError as err:
            body = None
            failures.append(f"body was not JSON ({err}): {entry['body']!r}")
        if body is not None:
            if not isinstance(body, dict):
                failures.append(f"body was {type(body).__name__}, expected an object")
            else:
                # Slack rejects nested values on a webhook trigger with
                # parameter_validation_failed, so keep the payload flat.
                nested = sorted(
                    k for k, v in body.items() if isinstance(v, (dict, list))
                )
                if nested:
                    failures.append(f"payload is not flat; nested keys: {nested}")
                else:
                    print(f"  ok  {case}: {entry['body']}")

    if failures:
        # Leave the captures in place; they are the evidence for debugging.
        print(f"\nSlack alert check FAILED for {case}:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        sys.exit(1)
    # Consume only this case, so several sends can share one stub.
    if rest:
        open(CAPTURE_FILE, "w", encoding="utf-8").writelines(rest)
    else:
        os.remove(CAPTURE_FILE)


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "sites":
        sites()
    elif command == "pins":
        pins()
    elif command == "serve":
        serve()
    elif command == "wait":
        wait()
    elif command == "verify" and len(sys.argv) > 2:
        verify(sys.argv[2])
    else:
        sys.exit(f"usage: {sys.argv[0]} (sites|pins|serve|wait|verify <case>)")
