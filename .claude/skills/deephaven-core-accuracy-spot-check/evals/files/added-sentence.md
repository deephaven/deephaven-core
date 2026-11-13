File: docs/python/conceptual/query-engine/parallelization.md (Concept guide), section "Within a single table".

Sentence as it read before this edit:

> When you run `source.update("Total = Price * Quantity")`, Deephaven divides the rows into groups and calculates each group on a different CPU core.

Sentence added directly after it in this edit:

> Deephaven only does this for tables with more than 1 million rows; smaller tables are computed on a single core.
