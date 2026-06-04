---
description: Sync margaretkennedy:main with deephaven-core:main
---

# Sync Fork with Upstream

This workflow syncs your fork's main branch with the upstream deephaven-core:main.

## Steps

// turbo
1. Fetch latest from upstream:
```bash
git fetch upstream
```

// turbo
2. Checkout main branch:
```bash
git checkout main
```

// turbo
3. Merge upstream/main into local main:
```bash
git merge upstream/main
```

// turbo
4. Push to your fork (force if needed):
```bash
git push origin main
```

If the push fails due to divergence, use:
```bash
git push --force origin main
```

## Notes

- **origin**: margaretkennedy/deephaven-core (your fork)
- **upstream**: deephaven/deephaven-core (main repo)
- Force push is safe on your fork's main since it should mirror upstream
