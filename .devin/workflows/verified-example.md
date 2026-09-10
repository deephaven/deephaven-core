---
description: Write a Deephaven code example and verify it by running it on a live instance
---

1. Invoke the `verified-code-example` skill to load the full instructions, then follow them.

2. Ask the user what the example should demonstrate, and in which language, if not already provided.

3. Follow the skill's steps in order. The non-negotiable parts:
   - Confirm a live Deephaven worker over MCP **before** writing any code. If none is reachable, stop and prompt the user to start one.
   - Run the exact example you intend to publish, then inspect its output — not just its exit code.
   - Diagnose any failure with the docs MCP before editing the code.
   - Fix what you can; hand off to the user after two failed attempts at the same error, with a hypothesis.

4. Clean up verification artifacts and report what you verified, what you fixed, and what you could not confirm.
