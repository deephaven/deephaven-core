---
description: Format, validate, and update snapshots for documentation
auto_execution_mode: 2
---

1. Run the formatter from the repo root:

// turbo

```sh
./docs/format
```

2. Run validation to check for broken links and compilation errors:

// turbo

```sh
./docs/validate
```

3. If validation fails due to missing snapshots, run the snapshotter:

```sh
./docs/updateSnapshots
```

4. Report any errors found and suggest fixes.
