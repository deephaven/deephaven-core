# Findings: Query parallelization concept guide (docs/python/conceptual/query-engine/parallelization.md)

Reviewer comments from the 2026-10-12 read of the page, to apply.

[OPEN] hard to follow · structure · Python · L61-86 ("What is and isn't parallelized"): The page explains parallelism at three levels (tables, rows, columns) and offers two controls (`with_serial`, barriers), but never puts the levels and the controls side by side. A reader assembles the model from four sections. Fix: open the section with one short table or list that pairs each level with what happens by default and what controls it, drawing on the sentences the section already has.
[OPEN] minor · structure · Python · L25 vs L63: The intro says Deephaven parallelizes "in three ways"; "What is and isn't parallelized" says "at two levels". Fix: use one framing in both places.
[OPEN] hard to follow · structure · Python · L80-84: The "Two other cases work differently" list (Deferred evaluation, Serialization you request) sits under the bold run-in "What you can control", but deferred evaluation is not a control. Fix: move deferred evaluation out from under that label (its own run-in label or heading), and fold "Serialization you request" into the controls text or drop it, since Controlling execution order covers it.
[OPEN] minor · style · Python · L67-74: The operations list is not parallel: two items begin with a category ("Column calculations in ...", "Filters in ...") and four with the operation name. Fix: give every item the shape "operation: what is split".
