---
description: Add a new documentation page to the sidebar
---

1. Ask for the file path of the new page if not provided.

2. Determine which sidebar to update:
   - If path contains `docs/python/` → use `docs/python/sidebar.json`
   - If path contains `docs/groovy/` → use `docs/groovy/sidebar.json`

3. Read the new page to get the `sidebar_label` from frontmatter (or `title` if no sidebar_label).

4. Determine the correct section in the sidebar based on the file path:
   - `reference/table-operations/` → Table Operations section
   - `reference/community-questions/` → Community Questions section
   - `how-to-guides/` → How-to Guides section
   - `conceptual/` → Conceptual section
   - etc.

5. Add the new entry to the appropriate section in sidebar.json:
   ```json
   {
     "label": "<sidebar_label from frontmatter>",
     "path": "<relative path from sidebar.json>"
   }
   ```

6. Run `/format-docs` to validate the changes.
