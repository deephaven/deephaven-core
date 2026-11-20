---
title: Ownership and closing
---

# Ownership and closing

Every table holds server resources until it is closed. Whoever awaits or resolves a {@link dh.PendingTable} owns the
result and closes it, and code that keeps a table it was lent takes its own with
{@link dh.ResolvedTable.copy | copy}.
