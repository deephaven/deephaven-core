---
title: TypeScript notes
---

# TypeScript notes

The types come from the `@deephaven/jsapi-types` package, while the API itself is loaded from the server it connects to.
A few places need the caller's help, such as narrowing what {@link dh.IdeSession.getObject | getObject} returns before
chaining on it.
