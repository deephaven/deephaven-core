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
     - Python scripting integration: `Integrations/src/main/java/io/deephaven/integrations/python/`
     - Groovy scripting integration: `engine/table/src/main/java/io/deephaven/engine/util/GroovyDeephavenSession.java`, `server/src/main/java/io/deephaven/server/console/groovy/`
   - Search the source to verify method signatures, parameters, and return types — don't infer them from the doc's own prose.
   - Check that code examples use correct, current syntax and API calls.
   - Verify that described behavior matches the actual implementation, not just what sounds plausible.
   - **For service/component names**, search `server/src/main/java/io/deephaven/server/` for the exact `*ServiceGrpcImpl` class name — don't guess between similarly named alternatives.
   - For configuration properties, search `Configuration/src/main/java/io/deephaven/configuration/` and `props/**/src/main/resources/*.prop` (e.g. `props/configs/src/main/resources/dh-defaults.prop`, `props/test-configs/src/main/resources/dh-tests.prop`) for actual property names — never invent one.
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
   - **Update-cycle framing:** Avoid "instant" or claims of no micro-batching — the update cycle is effectively micro-batching. Avoid a blanket "work proportional to what changed" too: that holds for simple incremental paths, but operations like refilter can force broader or full-table re-evaluation. Frame it per-operation instead of with one universal claim.
   - **Repo scope:** Flag any feature described as available in deephaven-core if it's actually Enterprise-only (Persistent Query lifecycle, Controller/Worker/Dispatcher model, kv store/etcd config) — those live in deephaven-ent, not here.

   - **For sizing recommendations, performance numbers, or "typical ranges":**
     - NEVER invent numbers. These require SME expertise or benchmarks.
     - Search existing docs for cited figures; if found, reference the source.
     - If no source exists, mark as "⚠️ Needs SME input" and suggest a reviewer from the SME matrix.
   - Flag any outdated or incorrect information.

4. **Style guide proofreading:** Apply the `deephaven-writing-style` skill for the full style guide — tone, quotes, links, page structure, proper noun capitalization, code formatting conventions, backticks, prose quality (active voice, jargon, audience calibration). That skill is the single source of truth; don't maintain a separate proper-noun, formatting, or punctuation list here.

   **Mechanical pattern checks — run these as literal Grep searches, don't rely on catching them by eye.** These specific mistakes have recurred across many reviews of this doc set, so treat them as required searches, not optional style intuition:
   - Search for `` `\.[a-z] `` (backtick, dot, lowercase letter) in the file. For every hit, confirm it's a genuine file extension or config key (`.parquet`, `.env`, `.yml`) and not a method/property reference in prose — a bare method name with **no leading dot** is this repo's actual convention (confirmed by corpus frequency: hundreds of bare mentions of `where`/`update`/`with_serial`/etc. vs. only isolated dot-prefixed outliers, each traceable to a specific bug). Flag every dot-prefixed method reference in prose (e.g. `.with_serial`, `.where`) for correction.
   - Search for backticked method-shaped identifiers (`snake_case` or `camelCase`, especially ones matching `with_`, `is_`, `from_`, `agg_`, `update`, `select`, `where`, etc.) and confirm each one appears inside a markdown link (`` [`name`](...) ``) at least once in the file. Flag any that are only ever mentioned bare — first mention of a method should link to its reference page or pydoc/javadoc anchor.
   - Search for a backticked identifier immediately followed by `()` outside of a fenced code block (e.g. `` `with_serial()` `` in prose) — flag it; parentheses belong in code, not prose.
   - **If you're unsure whether a pattern is actually "the project standard"** (including when a prior comment or your own assumption asserts one), don't trust the assertion alone — verify by counting real occurrences of both forms across `docs/python` and `docs/groovy` (e.g. `grep -rc` for each candidate form). A stated convention — including one written into this skill or `deephaven-writing-style` — can itself be wrong; corpus frequency is the actual authority.

   **Cross-language consistency check (when a sibling doc exists):** If reviewing `docs/python/.../X.md`, check whether `docs/groovy/.../X.md` exists, or vice versa. If so, diff the substantive claims between them — numeric thresholds, "cannot be used with..." restrictions, and any enumerated list ("N ways this works," "these methods are supported") — and flag any divergence that isn't explained by an actual language-level API difference. Verify each language's claims independently against that language's own source rather than assuming a claim already confirmed correct in one language's doc also holds for its sibling.

   **Completeness check (for docs that enumerate a fixed set of things):** When a doc lists mechanisms, config properties, or methods ("Deephaven parallelizes in N ways," a table of filter functions, etc.), independently derive the exhaustive list from source (grep for all static factory methods on the relevant class, all properties in the config file, all mechanisms documented in the related conceptual doc) and diff it against what the doc actually lists. Flag missing entries, not just wrong ones — an omission that leaves out a real, user-relevant capability is as much a defect as a false claim.

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
