---
description: Review documentation for technical accuracy, style, and missing links
---

> [!IMPORTANT]
> **Verification is mandatory.** Before confirming ANY claim is accurate or applying ANY fix:
> 1. Identify the authoritative reference (source code, reference docs, implementation)
> 2. Read that reference and quote the relevant text
> 3. Only then state whether the claim is accurate or apply the fix
> 
> Do NOT accept suggestions at face value — not from Copilot, not from user feedback, not from your own analysis.

1. Ask for the file path to review if not provided.

2. Read the documentation file.

3. **Technical accuracy review:**
   - Search the Deephaven source code to verify method signatures, parameters, and return types.
   - Check that code examples use correct syntax and API calls.
   - Verify that described behavior matches the actual implementation.
   - Flag any outdated or incorrect information.
   
   **Common accuracy pitfalls to check:**
   - **Execution context:** Do `transform` callbacks on live partitioned tables capture and reopen an execution context? (Required because new constituents arrive on update threads.)
   - **Materialization vs. direct access:** Does text claiming "direct access" actually involve a copy? Note: `to_pandas()` and `to_numpy()` materialize data; `Vector.toArray()` may return the backing array (no copy) per the interface contract, while `Vector.copyToArray()` guarantees a fresh copy. Be precise about which method is being discussed.
   - **Update graph semantics:** Are timing guarantees accurate? (1000ms is a target interval, not a deadline. Cycles can exceed it.)
   - **TableUpdate contract:** Does the description include row-shift and modified-column info? Are refilter scenarios acknowledged?
   - **Absolute statements:** Are comments like "This won't produce X" actually warnings? (Parallelism makes ordering non-deterministic, but serial execution can still produce sequential results.)
   - **API per format:** Are different formats (Parquet vs. CSV) shown with their distinct APIs, not combined into one row?
   - **`ii` behavior:** Is `ii` described as providing row position, not as making execution sequential?
   - **Python properties vs methods:** Check `@property` decorators in the source. Properties use `table.is_refreshing` (no parens); methods use `table.snapshot()` (with parens).
   - **Formula compilation:** Only direct column references bypass compilation. Don't say "simple formulas are pre-compiled" — complexity doesn't determine compilation path.
   - **Listener attachment conditions:** WhereListener can exist for static sources with refreshing filter dependencies, not just refreshing parent tables.
   - **Incremental evaluation claims:** Filters can trigger broader re-evaluation (refilter path), not just changed rows. Avoid overstating "only changed rows."

4. **Style guide proofreading (based on `.windsurf/rules`):**
   - Check for passive voice and suggest active alternatives.
   - Verify sentence case in headings.
   - Ensure bullet points have periods where needed.
   - Check that method names in prose don't have parentheses.
   - Verify proper nouns are capitalized (Core+, Enterprise, Persistent Query, etc.).
   - Ensure backticks are used for methods, classes, variables, and file paths.
   - Prefer em dashes over hyphens/en dashes for parenthetical statements, with spaces: `word — word`.

5. **Internal link review:**
   - Identify methods, classes, or concepts mentioned without links.
   - Suggest links to appropriate reference pages in `docs/{python,groovy}/reference/`.
   - Check that existing links are valid and point to the correct pages.
   - Ensure a "Related documentation" section exists (unless it's a landing page, overview, or blog).
   - **Before suggesting any new link:** Run `find_file_by_name` to confirm the target file exists.

6. Report findings organized by category with specific suggestions for fixes.

7. **Before applying any fixes:**
   - For each fix, show the reference source that confirms it is correct.
   - For any link additions, show the `find_file_by_name` result confirming the path.
   - Do NOT batch fixes without verification — verify each one individually.
