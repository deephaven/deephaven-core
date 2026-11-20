---
title: Values and filters
---

# Values and filters

A JavaScript number cannot hold every value a Deephaven column can, so 64-bit integers, dates and big numbers arrive
wrapped, as {@link dh.LongWrapper}, {@link dh.DateWrapper}, {@link dh.BigDecimalWrapper} and
{@link dh.BigIntegerWrapper}. Filters are built from {@link dh.FilterValue} into {@link dh.FilterCondition}s, which both
table APIs accept.
