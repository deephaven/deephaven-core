# TypeScript examples for the immutable table API

Small, compilable snippets showing how the chainable table API reads from TypeScript. They are
modeled on the examples in the Python server documentation, so the two can be compared side by
side. They are type-checked only; nothing here runs.

- `use-cases.ts`: the ordinary paths. Create or fetch a table, chain operations, await once, read
  data, close.
- `negatives.ts`: inputs the types are expected to reject, and a few they still accept.
- `pending-object.ts`: what `getObject` returns, a `PendingTable<Table> | Promise<any>` union, and
  how to await, narrow and chain on it.

To type-check them against the definitions generated from the current sources:

```
./gradlew :web-client-api:typescriptDefinitions
cd web/client-api/types/examples
npx -p typescript tsc -p tsconfig.json
```

`tsconfig.json` maps `@deephaven/jsapi-types` to the generated `build/ts-types/types.d.ts`, so the
imports read the way a consumer's would.

TODO: these should reach the published typedoc output (this package already builds it), either by
including them as-is or by rewriting them as examples meant for that. Nothing in the build
type-checks them yet.
