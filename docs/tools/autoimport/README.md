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

## Usage

### Step 1: Run the script in Deephaven

1. Start a Deephaven server
2. Copy `generate_autoimport_docs.py` to the data volume
3. Run the script in the Deephaven IDE:

```python
exec(open("/data/generate_autoimport_docs.py").read())
```

The script takes several minutes to complete as it scrapes JavaDocs for descriptions.

### Step 2: Copy output to docs

Copy the generated files from `/data/autoimport_output/` to the appropriate docs directory:

**For Python docs:**
```bash
cp /data/autoimport_output/*.md docs/python/reference/query-language/query-library/auto-imported/
```

**For Groovy docs:**
```bash
cp /data/autoimport_output/*.md docs/groovy/reference/query-language/query-library/auto-imported/
```

### Step 3: Update the index page

After copying, verify that the `index.md` file in each directory correctly references all category pages.

### Step 4: Format and validate

```bash
./docs/format
./docs/validate
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
