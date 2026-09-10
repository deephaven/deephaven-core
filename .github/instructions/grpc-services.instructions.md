---
applyTo:
  - "server/src/main/java/io/deephaven/server/table/ops/**"
  - "server/src/main/java/io/deephaven/server/hierarchicaltable/**"
  - "server/src/main/java/io/deephaven/server/partitionedtable/**"
  - "server/src/main/java/io/deephaven/server/console/**"
  - "server/src/main/java/io/deephaven/server/session/**"
  - "extensions/flight-sql/src/main/java/**"
---

# gRPC Service Guidelines

Server-side gRPC handlers turn untrusted client requests into engine operations, so they are a
security boundary. When adding or changing a handler (`server/src/.../table/ops/*GrpcImpl.java`, the
hierarchical/partitioned/console/input-table services, or a service-loaded `TicketResolver`), check
the following:

- **Validate every user-supplied expression the engine will compile.** Any request string that
  becomes a formula, filter, or selectable compiled as Java — `update`/`view`/`select`, `where`,
  `AggFormula`, `AggCountWhere`, rolling formulas, `updateView`/`format` node ops, etc. — must be run
  through `io.deephaven.engine.validation.ColumnExpressionValidator` **before** it reaches the engine.
  Strings that are parsed into structured types instead (`ColumnName`, `JoinMatch`, `Pair`,
  `SortColumn`, `RangeJoinMatch`) are safe by construction and enforced by `NameValidator`; a plain
  column-name argument does not need the expression validator, a compiled expression always does.
- **Validate the exact string the engine compiles, against the shape it compiles against.** If you
  substitute tokens (e.g. a rolling-formula param token), use the engine's own substitution
  (`FormulaUtil.replaceFormulaTokens`, a literal token-aware replace) — never `String.replaceAll`,
  which treats the user token as a regex and can make the validator inspect a different string than
  the engine runs. Build the validation prototype (`TableDefinition`) with the same column shapes the
  engine sees: grouped vs. scalar/vector columns, and any synthetic columns the engine injects (e.g.
  rollup `__FORMULA_DEPTH__` / `__FORMULA_KEYS__`). If the engine compiles the same expression at
  more than one shape (e.g. a rollup compiles each formula once per grouping prefix, from the base
  level down to the empty-key root, turning dropped keys from scalars into vectors), validate it at
  **every** such shape, not just one.
- **Keep validation consistent across equivalent paths.** The same proto message often reaches the
  engine through several services (e.g. `Aggregation` is used by `Aggregate`, `AggregateAll`,
  `RangeJoin`, and `Rollup`). Validate it identically everywhere; when a new service reuses a message,
  mirror the existing validation rather than re-deriving a partial version.
- **Fail closed when a `switch` that decides how to validate an expression hits an unknown case.**
  When a `switch` over a `oneof`/enum picks *how to validate* a user expression, its `default` (and
  `TYPE_NOT_SET`) must reject with `INVALID_ARGUMENT` rather than fall through — an unhandled case
  there means an expression would reach the engine unvalidated, which is a security bypass. Prefer
  this over an `if (typeCase == X)` that silently ignores everything else. This is a narrow,
  fail-closed rule for validation-dispatch switches only; it is **not** a blanket policy to throw on
  every unknown proto value. Fields that do not carry compiled expressions must still follow normal
  proto compatibility — tolerate unknown enum/`oneof` values and deprecate rather than remove old
  ones — so older and newer clients and servers keep interoperating.
- **Validate request shape.** In `validateRequest`, use `GrpcErrorHelper.checkHasField` /
  `checkRepeatedFieldNonEmpty` / `checkHasNoUnknownFields` and `Common.validate(...)` on every ticket
  reference. `checkHasNoUnknownFields` is what rejects unknown/renamed proto fields.
- **Enforce authorization.** Every operation must call its
  `authWiring.checkPermission<Operation>(...)` before returning a result.
- **Surface failures as `INVALID_ARGUMENT`.** Throw via `Exceptions.statusRuntimeException` or a plain
  `IllegalArgumentException`; work performed inside a `SessionState` export is sanitized to
  "Details Logged w/ID" (INVALID_ARGUMENT) by the `ObfuscatingErrorTransformer`, so assert on the
  status code, and on that message when confirming a failure came through the export/validator path.
- **Cover it with gRPC-level tests** (extend `GrpcTableOperationTestBase`): one benign expression that
  is accepted and one disallowed expression that must be rejected. When the change closes a bypass,
  write the test so it fails on the pre-fix code (verify by reverting) — a test that passes either way
  proves nothing.
