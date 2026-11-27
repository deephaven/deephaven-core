---
title: Pending tables
---

# Pending tables

A {@link dh.PendingTable} is a table still being made on the server: it can be awaited, and more operations can be
chained on it before it exists. It is valid only in the task that created it, so chain on it, await it or
{@link dh.PendingTable.resolve | resolve} it before control returns to the event loop.
