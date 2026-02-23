---
description: Create Groovy version of a Python documentation page
---

1. Ask the user for the Python file path if not provided.

2. Read the Python documentation file.

3. Convert to Groovy conventions:
   - Change method names from `snake_case` to `camelCase`
   - Change variable names from `snake_case` to `camelCase`
   - Update imports to Groovy/Java equivalents
   - Convert Python syntax to Groovy syntax in code blocks
   - Update internal links to point to Groovy reference pages

4. Create the file in the equivalent Groovy directory:
   - `docs/python/...` → `docs/groovy/...`

5. Check if the page needs to be added to `docs/groovy/sidebar.json`:
   - Find the equivalent section in the sidebar
   - Add the new page entry

6. Run `/format-docs` to format and validate.
