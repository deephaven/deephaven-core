---
description: Create Groovy version of a Python documentation page
---

1. Ask the user for the Python file path if not provided.

2. Read the Python documentation file.

3. Convert to Groovy conventions:
   - Change variable names from `snake_case` to `camelCase`
   - Update internal links to point to Groovy reference pages

4. **Verify code examples against JavaDocs** (do NOT assume direct equivalents):
   - Look up each Python method in the JavaDocs to find the correct Groovy/Java equivalent.
   - Python and Groovy APIs may differ significantly — verify method names, parameters, and return types.
   - Check existing Groovy reference pages in `docs/groovy/reference/` for correct usage patterns.
   - Update imports to Groovy/Java equivalents based on actual API.
   - If no direct equivalent exists, note this and ask the user how to proceed.

5. Create the file in the equivalent Groovy directory:
   - `docs/python/...` → `docs/groovy/...`

6. Check if the page needs to be added to `docs/groovy/sidebar.json`:
   - Find the equivalent section in the sidebar
   - Add the new page entry

7. Run `/format-docs` to format and validate.
