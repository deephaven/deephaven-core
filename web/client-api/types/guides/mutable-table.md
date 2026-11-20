---
title: Working with a dh.Table
---

# Working with a dh.Table

A {@link dh.Table} is one object whose filters, sorts and custom columns can change over time, firing events as they do,
which suits UI state such as a grid. Changes apply asynchronously, and closing the table releases every server-side
table it made.
