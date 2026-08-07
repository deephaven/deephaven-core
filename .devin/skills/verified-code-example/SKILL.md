---
name: verified-code-example
description: Write Deephaven code examples that are executed and debugged against a live local Deephaven instance before being handed back to the user
---

Never hand a Deephaven code example to a human until it has actually run. Every example produced under this skill must execute cleanly on a live Deephaven worker first.

Use this skill whenever you are asked to write, update, or fix a Deephaven code example, snippet, doc code block, or tutorial script.

## 1. Confirm a live Deephaven instance before writing anything

Do this **first**, before generating code. Do not defer it until you have an example ready.

1. List the sessions the MCP server can reach:

```
mcp1_sessions_list
```

2. Pick a candidate session. Prefer a `COMMUNITY` session whose `programming_language` matches the example language. Verify it is actually reachable:

```
mcp1_session_details(session_id="<session_id>", attempt_to_connect=true)
```

3. Check the response. Continue only if `success` is `true`, `available` is `true`, and `liveness_status` is `"ONLINE"`.

**If no session is reachable, stop and prompt the user.** Do not write the example, do not guess, and do not silently skip verification. Tell the user exactly what is missing and offer the ways to get a worker running:

- Start the local server from this repo: `./gradlew server-jetty-app:run` (add `-Pgroovy` for a Groovy worker). The PSK is printed in the startup log.
- Or start a container: `docker compose up` from the repo root.
- Or let you create a throwaway worker via `mcp1_session_community_create`, which requires Docker or the `deephaven-server` pip package. Ask before doing this — it consumes real resources, and you must clean it up with `mcp1_session_community_delete` when finished.

Wait for the user. Once they confirm, re-run steps 1 and 2 before proceeding.

**Language check.** A Python example needs a Python worker and a Groovy example needs a Groovy worker. If the only live session is the wrong language, say so and ask the user to start the right one rather than translating the example to fit the worker you happen to have.

## 2. Know the environment before you write

Ping the worker for what is actually installed so you do not write against libraries that are not there:

```
mcp1_session_pip_list(session_id="<session_id>")
mcp1_session_tables_list(session_id="<session_id>")
```

Prefer `deephaven` APIs already present in the worker. If the example genuinely needs a package that is missing, say so in your final response rather than assuming the user will install it.

## 3. Write the example

Follow the repo documentation style guide at `docs/python/templates/_style-guide.md`.

- Include every import the snippet needs. A reader must be able to paste the block and run it.
- Use meaningful variable and table names.
- Keep the example self-contained: generate or synthesize its own data unless the point of the example is reading from a specific external source.
- Match the fenced-block attributes the docs use (`python`, `python skip-test`, `python syntax`, `python test-set=N order=...`) to how the example will actually be tested.

## 4. Run it

Execute the exact code you intend to publish — not a modified variant, not a subset:

```
mcp1_session_script_run(session_id="<session_id>", script="<the example, verbatim>")
```

If the example creates tables, confirm they are real and shaped as described:

```
mcp1_session_tables_list(session_id="<session_id>")
mcp1_session_table_data(session_id="<session_id>", table_name="<table>", max_rows=10)
mcp1_session_tables_schema(session_id="<session_id>", table_names=["<table>"])
```

**Verify the claims, not just the exit code.** If your prose says a column is a `double`, check the schema. If it says the result has one row per symbol, look at the data. A script that runs without error but produces the wrong output is still a broken example.

Use a unique prefix for table and variable names so you do not collide with the user's existing session state, and mention any names you created in your final response.

## 5. Diagnose failures with the docs MCP

When the run fails, **consult the docs MCP before editing the code**. Guessing at a fix and re-running is the slowest way to converge, and it is how examples end up subtly wrong.

Query the documentation assistant with the actual error and the actual code:

```
mcp0_docs_chat(
    prompt="<the error message, plus the line that produced it, plus what the code is trying to do>",
    programming_language="python",
    deephaven_core_version="<version from session_details>"
)
```

Pass the version and language you got back in step 1 so the answer matches the worker you are running against, not some other release.

Write a specific prompt. `"Why does my query fail?"` gets you nothing. `"deephaven.update_by with rolling_sum_tick raises 'RuntimeError: Cannot find column Price' — how do I specify the operation columns?"` gets you the signature.

Use follow-ups rather than starting over. Pass prior turns via `history` so the assistant keeps the context of your example.

The docs MCP is the first resort, not the only one. Combine it with:

- **The source in this repo** — `py/server/deephaven/` for the Python API, `engine/api/` for table semantics. The source is authoritative when it disagrees with the docs.
- **The live worker** — `mcp1_session_tables_schema` and `mcp1_session_table_data` tell you what the data actually looks like, which often explains a type or column error outright.
- **`mcp1_session_pip_list`** — confirms whether a module is genuinely absent versus misnamed.

If the docs and the running worker disagree, trust the worker and note the discrepancy in your final response. That is a docs bug worth reporting.

## 6. Fix what you can, escalate what you cannot

Once you have diagnosed the cause, fix it and re-run. Iterate until it passes. Fix without asking:

- Syntax errors, indentation, unbalanced delimiters
- Missing or wrong imports
- Wrong function, method, or argument names — confirm the correct signature via `mcp0_docs_chat` or `py/server/deephaven/` before re-running
- Type mismatches in query strings and column expressions
- Deprecated or renamed APIs
- Undefined names and typos
- Off-by-one and obviously wrong literals

**Do not paper over a failure.** Wrapping the snippet in `try`/`except`, deleting the failing line, or downgrading the example to something trivial that happens to run is not a fix. Address the actual cause.

**Two strikes on the same issue, then hand it off.** If you have attempted a fix for a given error twice and it still fails, stop retrying. Looping past that point rarely converges and burns the user's time. Pass it to a human with:

- The failing code, as it stands.
- The exact error text from each attempt.
- Each fix you tried and why you thought it would work.
- What the docs MCP said, and where its answer did not match the worker's behavior.
- Your best hypothesis about the root cause — a changed API signature, a missing dependency, an engine bug, a wrong assumption in the example's premise — even if you are not confident. A stated guess is more useful to the reviewer than silence.

This applies per issue, not per example. If you fix one error and a genuinely different one surfaces, that is progress: keep going, with a fresh two-attempt budget for the new error. If the same error keeps reappearing in different disguises, treat it as one issue.

Stop and ask the user when:

- The fix would change what the example is meant to demonstrate.
- The failure is caused by missing credentials, missing data, or missing infrastructure.
- Two attempts at the same error have failed.
- The failure looks like a genuine bug in Deephaven rather than in your example.

Report any issue that took more than one attempt in your final response, even if you eventually fixed it. It is a useful signal about the API's ergonomics.

## 7. Clean up and report

1. Drop tables or variables you created purely for verification, unless the user wants them.
2. Delete any worker you created yourself with `mcp1_session_community_delete`. Never delete a session you did not create.
3. In your final response, state:
   - Which session you ran against and its Deephaven version.
   - That the example executed successfully, and what you verified beyond "it did not error."
   - Any bugs you found and fixed.
   - Any place the docs contradicted the running worker.
   - Anything you could not verify, and why.

## Definition of done

- [ ] A live session was confirmed **before** the example was written
- [ ] The published code ran verbatim on that session
- [ ] Output was inspected, not just the absence of an error
- [ ] Every failure was diagnosed via the docs MCP before code was edited
- [ ] Every fixable bug was fixed at its root cause
- [ ] No single error was retried more than twice
- [ ] Unfixable blockers were handed off with the error text, the attempted fixes, and a hypothesis
- [ ] Verification artifacts and self-created sessions were cleaned up

Never describe an example as complete, working, or ready if it has not run. If verification was impossible, say that plainly instead of implying the code was tested.
