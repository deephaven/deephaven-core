File: docs/python/how-to-guides/use-deephaven-learn.md (How-to guide), introduction.

Paragraph as it read before this edit:

> The `deephaven.learn` gather-scatter operations work on both static and dynamic real-time tables. For static tables, the rows of the table are separated into batches. These batches are processed in sequence. For real-time tables, only the rows of the table that changed during the most recent update cycle are separated into batches and processed.

Paragraph after this edit (only the first sentence was changed):

> The `deephaven.learn` gather-scatter operations work on static tables and on ticking tables that are add-only or blink, because they use row keys internally. For static tables, the rows of the table are separated into batches. These batches are processed in sequence. For real-time tables, only the rows of the table that changed during the most recent update cycle are separated into batches and processed.
