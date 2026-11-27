# @deephaven/jsapi-types

Generated type definitions for the Deephaven JS client API

## Building the documentation

`./gradlew :web-client-api:types:assemble` builds the typedoc site into `build/typedoc/documentation`, along with the
npm package. `typedoc.json` holds the options, including the guides to publish and their sidebar order; a guide in
`guides/` that it does not list is not published.

## Guides and examples

`guides/` holds the markdown guides. They embed code from `examples/` by region, with
`{@includeCode ../examples/use-cases.ts#name}` and a matching `// #region name` ... `// #endregion name` in the example
file.

- `examples/use-cases.ts`: the examples the guides embed. The docs build type-checks them, and
  `./gradlew :web-client-api:types:check` runs each one against a server started for the purpose, failing if any
  rejects.
- `examples/negatives.ts`: inputs the types are expected to reject, and a few they still accept. Type-checked only.
- `examples/pending-object.ts`: what `getObject` returns, a `PendingTable<Table> | Promise<any>` union, and how to
  await, narrow and chain on it. Type-checked only.

To type-check the examples locally against the definitions generated from the current sources:

```
./gradlew :web-client-api:typescriptDefinitions
npx -p typescript@5.9.3 tsc -p examples/tsconfig.json
```
