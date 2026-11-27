---
title: Errors and failure events
---

# Errors and failure events

Failures show up differently in the two table APIs. A {@link dh.Table} applies changes asynchronously, and fires
{@link dh.Table.EVENT_REQUEST_FAILED} when one fails. {@link dh.TableOperations} never throw: a failure, including an
invalid argument, rejects the {@link dh.PendingTable} the operation returned.
