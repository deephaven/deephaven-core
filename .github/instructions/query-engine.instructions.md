---
applyTo:
  - "engine/table/**"
  - "engine/rowset/**"
  - "engine/chunk/**"
---

# Query Engine Guidelines

The engine processes large, ticking datasets on the hot path, so data-movement code must be written
for throughput. When adding or changing engine internals (`engine/table`, `engine/rowset`,
`engine/chunk`, aggregation/join/update-by operators, `ColumnSource`s, kernels), follow these rules:

- **Read data in bulk (chunked), not cell-by-cell.** Pull values through `ColumnSource` /
  `RowSequence` into `Chunk`s and operate on whole chunks; do not loop calling scalar
  `get`/`getPrev` per row key. Bulk access is the design intent of `Chunk` and the fill APIs.
- **Avoid per-cell virtual calls.** A megamorphic call in an inner loop defeats the JIT. Dispatch
  once per chunk to a type-specialized **kernel** rather than once per element; if no suitable kernel
  exists, write one (see the generated single-column kernel families and their dispatchers) instead
  of funneling per-cell calls through an interface.
- **Allocate per operation, not per chunk.** Create a reusable *context* object (e.g.
  `ChunkSource.FillContext` / `GetContext`, aggregation or kernel state) once, before iterating, and
  reuse it across every chunk in the loop. Do not allocate chunks, arrays, or boxed values inside the
  per-chunk body. Context objects are `SafeCloseable` — close them (try-with-resources).
- **Batch `RowSet` operations.** Use range- and chunk-oriented `RowSet` / `RowSetBuilder` /
  `WritableRowSet` APIs (`insertRange`, `insert(RowSet)`, `RowSequence` iteration, sequential/random
  builders). Avoid per-key `get`/`find`/`insert`/`remove` in a loop; presize builders and destinations
  to the source size (an open-hash set iterated into a default-sized destination can go
  ~quadratic — see the `RspBitmap` intersect history).
- **Keep `RowSet` operations O(n); never add a quadratic path.** When modifying or adding a `RowSet`
  operation, confirm its complexity is linear in the number of rows/ranges touched. A per-element
  `find`/`get`/`insert` inside a loop over another set is the classic quadratic trap — restructure
  to a single linear merge/scan instead. If you cannot make an operation O(n), flag it rather than
  shipping a quadratic path.
