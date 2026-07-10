---
description: Review documentation for technical accuracy, style, and missing links
---

1. Ask for the file path to review if not provided.

2. Read the documentation file.

3. **Technical accuracy review — VERIFY BEFORE TRUSTING:**

   **CRITICAL RULE:** For EVERY technical claim, search the source code FIRST. Do not assume something is correct because it looks plausible. If you cannot find verification, flag it as "UNVERIFIED."

   **This applies to BOTH reviewing AND writing:**
   - When reviewing: verify existing content against source before approving.
   - When writing/editing: search source FIRST, then write. Never write from memory.

   **Java/Python/Groovy code:**
   - Search for method signatures, parameters, and return types in source code before confirming they're correct.
   - Verify class names exist: `grep_search` for the class name in `engine/`, `extensions/`, or relevant module.
   - Verify imports: search for actual import paths — do not assume package names.
   - For EACH API call in an example, search for that method in source. If not found, flag it.
   - Read the actual source implementation to verify described behavior — do not trust prose descriptions.

   **Shell scripts, CLI, and configuration:**
   - Search `bin/` directory for actual script names and paths (e.g., `bin/start-deephaven`).
   - Verify environment variable names by searching the startup scripts — do NOT assume variable names.
   - Check CLI flags/arguments against actual script implementations.
   - Verify file paths exist in the repository structure.
   - Search for configuration property names in actual config files or property loaders.

   **Docker and deployment:**
   - Verify Docker image names, tags, and environment variables against actual Dockerfiles.
   - Check docker-compose examples against real compose files in the repository.
   - Verify port numbers, volume mounts, and network configurations.

   **General verification checklist:**
   - [ ] Every method/function name verified against source
   - [ ] Every parameter name and type verified
   - [ ] Every import statement verified against actual package structure
   - [ ] Every configuration property verified
   - [ ] Every script path and variable name verified
   - [ ] Every CLI command verified against actual scripts
   - [ ] Every internal link target file exists
   - [ ] Every external link flagged for manual verification

   **Operational claims — DO NOT INVENT:**

   The following have no source code to verify — never generate these from memory:
   - Sizing recommendations (heap sizes, RAM percentages, worker counts)
   - Performance benchmarks or "typical ranges"
   - Hardware requirements or capacity planning numbers
   - Tuning parameters or operational guidelines

   For any sizing/performance claim, check:
   - Is there an existing benchmark or sizing guide to cite?
   - Is this documented in official Deephaven materials?
   - If no source exists, flag as: `[TBD - needs SME input]`

   **Red flags** — always flag these for review:
   - Tables with sizing recommendations (GB, %, "typical range")
   - Statements like "typically," "usually," "in most cases" without citation
   - Any numeric recommendation that isn't from source code defaults

4. **Style guide proofreading (based on `.windsurf/rules`):**
   - Check for passive voice and suggest active alternatives.
   - Verify sentence case in headings.
   - Ensure bullet points have periods where needed.
   - Check that method names in prose don't have parentheses.
   - Verify proper nouns are capitalized (Core+, Enterprise, Persistent Query, etc.).
   - Ensure backticks are used for methods, classes, variables, and file paths.
   - Prefer em dashes over hyphens/en dashes for parenthetical statements, with spaces: `word — word`.

5. **Link review — VERIFY EVERY LINK:**

   **Internal links:**
   - Before suggesting a link, use `find_by_name` or `list_dir` to confirm the target file exists.
   - Do NOT suggest links to pages that might exist — verify first or flag as "LINK UNVERIFIED."
   - For existing links, verify the target path exists in the repository.
   - Ensure a "Related documentation" section exists (unless it's a landing page, overview, or blog).

   **External links:**
   - Flag external URLs for manual verification — do not assume they are valid.
   - For Javadoc links, verify the class/method exists in the codebase first.

6. **Report findings** organized by category:
   - **ERRORS:** Verified incorrect information (searched and found mismatch)
   - **UNVERIFIED:** Code/config claims that could not be verified (searched but not found)
   - **NEEDS SME:** Operational claims (sizing, performance, benchmarks) with no citable source
   - **STYLE:** Style guide violations
   - **LINKS:** Missing or broken links
   
   For each finding, include the search you performed and what you found (or didn't find).
