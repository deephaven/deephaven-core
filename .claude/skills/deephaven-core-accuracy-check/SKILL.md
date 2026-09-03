---
name: deephaven-core-accuracy-check
description: Review deephaven-core (Community) documentation for technical accuracy, style, and missing links. Use this whenever a developer asks you to review, check, proofread, or verify a doc against source for the deephaven-core repo specifically. Do not use for deephaven-ent/iris docs — that repo has a separate skill (deephaven-enterprise-accuracy-check) with different paths, pitfalls, and vocabulary.
allowed-tools: Read, Grep, Glob, Edit, Bash(git diff *)
---

> [!IMPORTANT]
> **Verification is mandatory.** Before confirming ANY claim is accurate or applying ANY fix:
> 1. Identify the authoritative reference (source code, reference docs, implementation).
> 2. Read that reference and quote the relevant text.
> 3. Only then state whether the claim is accurate or apply the fix.
>
> Do NOT accept a claim at face value — not from the doc author, not from prior review comments, not from your own initial read of the code. If you can't locate an authoritative reference, say so explicitly rather than assuming the doc is correct.

1. Ask for the file path to review if not provided.

2. Read the documentation file.

3. **Technical accuracy review:**
   - **For EVERY code snippet**, search the source code FIRST. Never write or "correct" an example from memory.
     - Engine/server code: `engine/`, `server/`, `extensions/`
     - Python API: `py/server/deephaven/`, `py/client/pydeephaven/`
     - Groovy/Python scripting integration: `Integrations/src/main/java/io/deephaven/integrations/{python,groovy}/`
   - Search the source to verify method signatures, parameters, and return types — don't infer them from the doc's own prose.
   - Check that code examples use correct, current syntax and API calls.
   - Verify that described behavior matches the actual implementation, not just what sounds plausible.
   - **For service/component names**, search `server/src/main/java/io/deephaven/server/` for the exact `*ServiceGrpcImpl` class name — don't guess between similarly named alternatives.
   - For configuration properties, search `Configuration/src/main/java/io/deephaven/configuration/` and `props/*.prop` files for actual property names — never invent one.
   - For gRPC/proto examples, verify against `proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/`.
   - If you cannot find the source to verify a snippet, flag it: "⚠️ Could not verify: [snippet]"

   **Common accuracy pitfalls to check specifically:**
   - **Execution context:** Do `transform` callbacks on live partitioned tables capture and reopen an execution context? (Required because new constituents arrive on update threads.)
   - **Materialization vs. direct access:** Does text claiming "direct access" actually involve a copy? (`toArray()`, `to_pandas()`, `to_numpy()` all materialize data.)
   - **Update graph semantics:** Are timing guarantees accurate? (1000ms is a target interval, not a deadline. Cycles can exceed it.)
   - **TableUpdate contract:** Does the description include row-shift and modified-column info? Are refilter scenarios acknowledged?
   - **Absolute statements:** Are comments like "this won't produce X" actually warnings? (Parallelism makes ordering non-deterministic, but serial execution can still produce sequential results.)
   - **API per format:** Are different formats (Parquet vs. CSV) shown with their distinct APIs, not combined into one row?
   - **`ii` behavior:** Is `ii` described as providing row position, not as making execution sequential?
   - **Python properties vs. methods:** Check `@property` decorators in the source. Properties use `table.is_refreshing` (no parens); methods use `table.snapshot()` (with parens).
   - **Formula compilation:** Only direct column references bypass compilation. Don't say "simple formulas are pre-compiled" — complexity doesn't determine compilation path.
   - **Listener attachment conditions:** `WhereListener` can exist for static sources with refreshing filter dependencies, not just refreshing parent tables.
   - **Incremental evaluation claims:** Filters can trigger broader re-evaluation (refilter path), not just changed rows. Avoid overstating "only changed rows."
   - **Update-cycle framing:** Avoid "instant" or claims of no micro-batching — the update cycle is effectively micro-batching. Correct framing: work proportional to what changed, not the size of the data.
   - **Repo scope:** Flag any feature described as available in deephaven-core if it's actually Enterprise-only (Persistent Query lifecycle, Controller/Worker/Dispatcher model, kv store/etcd config) — those live in deephaven-ent, not here.

   - **For sizing recommendations, performance numbers, or "typical ranges":**
     - NEVER invent numbers. These require SME expertise or benchmarks.
     - Search existing docs for cited figures; if found, reference the source.
     - If no source exists, mark as "⚠️ Needs SME input" and suggest a reviewer from the SME matrix.
   - Flag any outdated or incorrect information.

4. **Style guide proofreading:** Apply the `deephaven-writing-style` skill for the full style guide — proper noun capitalization, code formatting conventions, backticks, prose quality (active voice, jargon, audience calibration). That skill is the single source of truth; don't maintain a separate proper-noun or formatting list here. In addition, specific to review:
   - Prefer em dashes over hyphens/en dashes for parenthetical statements, with spaces: `word — word`.

5. **Internal link review:**
   - Identify methods, classes, or concepts mentioned without links.
   - Suggest links to appropriate reference pages in `docs/{python,groovy}/reference/`.
   - Check that existing links are valid and point to the correct pages.
   - Ensure a "Related documentation" section exists (unless it's a landing page, overview, or blog).
   - **Before suggesting any new link:** confirm the target file actually exists in the repo (search/list the directory for it) rather than assuming a path is correct by pattern-matching similar pages.

6. Report findings organized by category with specific suggestions for fixes.

7. **Before applying any fixes:**
   - For each fix, show the reference source (quoted, briefly) that confirms it's correct.
   - For any link additions, show what confirmed the target file's existence.
   - Do NOT batch fixes without verification — verify each one individually, even under time pressure.
