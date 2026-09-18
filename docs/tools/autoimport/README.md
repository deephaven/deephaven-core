# Auto-import Documentation Generator

This tool generates documentation for Deephaven's auto-imported query language functions.

## Overview

The tool introspects `QueryLibraryImportsDefaults` to discover all auto-imported classes, methods, and constants, then scrapes JavaDocs for descriptions and generates markdown documentation files.

## Output

The tool generates separate markdown files for each category:

- `basic.md` - Array manipulation, counting, null handling
- `math.md` - Mathematical operations, statistics, trigonometry
- `time.md` - Date and time utilities
- `logic.md` - Boolean operations
- `parse.md` - String-to-primitive parsing
- `sort.md` - Sorting functions
- `search.md` - Binary search utilities
- `gui.md` - Color utilities and constants
- `constants.md` - Null values, infinity, numeric limits
- `java.md` - Java standard library classes
- `data-types.md` - Type casting utilities

## Quick Start (Step-by-Step)

### Step 1: Create a data folder

Open Terminal and run:

```bash
mkdir -p ~/deephaven-data
```

### Step 2: Copy the script to the data folder

From the `deephaven-core` repository root:

```bash
cp docs/tools/autoimport/generate_autoimport_docs.py ~/deephaven-data/
```

### Step 3: Start a Deephaven server

```bash
docker run --rm -it -v ~/deephaven-data:/data -p 10000:10000 ghcr.io/deephaven/server:latest
```

> **Port already in use?** If you see an error about port 10000, use a different port:
>
> ```bash
> docker run --rm -it -v ~/deephaven-data:/data -p 10001:10000 ghcr.io/deephaven/server:latest
> ```
>
> Then use http://localhost:10001 instead of http://localhost:10000 below.

### Step 4: Run the script in Deephaven

1. Open your browser to http://localhost:10000
2. In the console panel, paste this and press Enter:

```python
exec(open("/data/generate_autoimport_docs.py").read())
```

3. **Wait for "DONE!" message** - this takes several minutes because it downloads JavaDoc pages.

### Step 5: Copy output to docs

Open a **new terminal window** (Cmd+T or Cmd+N) and run:

From the `deephaven-core` repository root:

> **Important:** Don't overwrite `index.md` - it has language-specific content. If prompted, skip it or restore it from git.

**For Python docs:**

```bash
cp ~/deephaven-data/autoimport_output/*.md docs/python/reference/query-language/query-library/auto-imported/
```

**For Groovy docs:**

```bash
cp ~/deephaven-data/autoimport_output/*.md docs/groovy/reference/query-language/query-library/auto-imported/
```

### Step 6: Stop the server

Go back to the terminal running Docker and press `Ctrl+C`.

### Step 7: Format and validate

From the `deephaven-core` repository root:

```bash
./docs/format
./docs/validate
```

If validate reports missing snapshots:

```bash
./docs/updateSnapshots
```

## Customization

### Adding new categories

Edit the `CATEGORY_FILTERS` dictionary in `generate_autoimport_docs.py` to add new category mappings.

### Modifying page templates

Edit the `generate_page_header` and `generate_page_footer` functions to customize the markdown output.

## Dependencies

The script requires:

- `beautifulsoup4` - For HTML parsing (installed automatically)
- `requests` - For HTTP requests (installed automatically)
- Deephaven Python client libraries (available in Deephaven session)

## CI Integration

A GitHub Action (`.github/workflows/autoimport-docs-check.yml`) monitors the auto-import documentation for sync issues.

### What the sync check does

`CheckAutoImportDocSync` (in `engine/table/src/test/java/…`) compares two things:

1. **The live code** — it uses Java reflection to list every `public static` method and field from the classes in `QueryLibraryImportsDefaults.statics()` that are covered by the generator (those matching `DOCUMENTED_CLASS_PREFIXES`).
2. **The committed docs** — it scans every `.md` file in `docs/{python,groovy}/reference/query-language/query-library/auto-imported/` and collects every name that appears in a `FUNCTION` or `CONSTANT` table row.

If the two sets don't match, the check reports:

- **"In code but missing from docs"** — a method or constant exists in the Java source but has no row in the markdown tables. This means the docs are stale and need regenerating.
- **"In docs but absent from all statics()"** — a name appears in the markdown tables but no longer exists in the Java source. This usually means a method was renamed or removed without updating the docs.

Both Python and Groovy doc directories are checked independently.

### When it runs

- **Weekly schedule** — every Sunday at 3 AM UTC, to catch silent drift between releases.
- **PR trigger** — prints an informational reminder (but does not fail the build) whenever `QueryLibraryImportsDefaults.java` is modified in a pull request.
- **Manual trigger** — run on demand from the GitHub Actions tab.

### How to trigger manually

1. Go to **Actions → "Auto-Import Docs Sync Check"**
2. Click **"Run workflow"**

### How to check locally

Run this from the repository root (requires Java 17+, no Docker or running server needed):

```bash
./gradlew :engine-table:checkAutoImportSync
```

Sample output when everything is in sync:

```
=== python: in sync ===
=== groovy: in sync ===
```

Sample output when docs are stale:

```
=== python: OUT OF SYNC ===
  In code but missing from docs (2): [newMethod, anotherMethod]
=== groovy: OUT OF SYNC ===
  In code but missing from docs (2): [newMethod, anotherMethod]
```

### What to do when it fails

**Case 1 — "In code but missing from docs"**

One or more auto-imported functions or constants exist in the Java source but are not documented. Follow the [Quick Start](#quick-start-step-by-step) steps above to regenerate the docs, then commit the updated markdown files.

**Case 2 — "In docs but absent from all statics()"**

One or more names in the markdown tables no longer exist in `QueryLibraryImportsDefaults.statics()`. Either:

- A method was renamed or removed — delete or correct the row in the relevant `.md` file(s), or
- The class itself was removed from `statics()` — remove all rows for that class from the docs.

After fixing either case, re-run `./gradlew :engine-table:checkAutoImportSync` locally to confirm the check passes before pushing.

### Failure notifications

On a scheduled or manual run, if the check fails and the workflow is running in the `deephaven` organization, a Slack message is posted to the `#ddl-devrel` channel with a link to the failed run.

## Notes

- The script must run inside a Deephaven session because it uses `jpy` to introspect Java classes.
- Scraping JavaDocs can be slow; the script caches requests to avoid redundant downloads.
- Some methods may show "No description." if JavaDoc documentation is missing.
