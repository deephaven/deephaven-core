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

Edit the `generate_page_header()` and `generate_page_footer()` functions to customize the markdown output.

## Dependencies

The script requires:

- `beautifulsoup4` - For HTML parsing (installed automatically)
- `requests` - For HTTP requests (installed automatically)
- Deephaven Python client libraries (available in Deephaven session)

## Notes

- The script must run inside a Deephaven session because it uses `jpy` to introspect Java classes
- Scraping JavaDocs can be slow; the script caches requests to avoid redundant downloads
- Some methods may show "No description." if JavaDoc documentation is missing
