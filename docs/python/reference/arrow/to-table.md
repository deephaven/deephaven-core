---
title: to_table
---

`to_table` converts a PyArrow table to a Deephaven table.

## Syntax

```python syntax
to_table(pa_table: pyarrow.Table, cols: Sequence[str] | None = None) -> Table
```

## Parameters

<ParamTable>
<Param name="pa_table" type="pyarrow.Table">

A PyArrow table. Every column that `to_table` converts must have a [supported column type](#supported-column-types).

</Param>
<Param name="cols" type="Sequence[str]" optional>

The columns to convert, in the order they appear in the result. Default is `None`, which means all columns. An empty sequence also converts all columns.

</Param>
</ParamTable>

## Returns

A Deephaven table.

`to_table` raises a `DHError` if a converted column has a type that is not in [Supported column types](#supported-column-types). If `cols` names a column that `pa_table` does not have, PyArrow raises a `KeyError` instead.

## Supported column types

Starting in version 43.0, [`deephaven.arrow.is_deephaven_compatible`](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.is_deephaven_compatible) returns whether `to_table` supports a given PyArrow type.

`to_table` accepts columns of these PyArrow types:

- The types listed in [`deephaven.arrow.SUPPORTED_ARROW_TYPES`](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.SUPPORTED_ARROW_TYPES). `to_table` maps each of these directly to a Deephaven column type.
- These parameterized types, which `SUPPORTED_ARROW_TYPES` does not list: 128-bit and 256-bit decimals, lists, list views, fixed-size lists, maps, fixed-size binary, unions, and timestamps with a time zone. The element types of a list, map, or union must also be supported.
- Starting in version 43.0, dictionary-encoded and run-end-encoded columns whose values have one of the types above. The result column holds the decoded values. [`deephaven.arrow.SUPPORTED_ARROW_ENCODINGS`](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.SUPPORTED_ARROW_ENCODINGS) lists these encodings.

The types in `SUPPORTED_ARROW_TYPES` map to these Deephaven column types:

| PyArrow type                                                              | Deephaven column type                    |
| ------------------------------------------------------------------------- | ---------------------------------------- |
| `pa.null()`                                                               | `java.lang.Object`                       |
| `pa.bool_()`                                                              | `java.lang.Boolean`                      |
| `pa.int8()`                                                               | `byte`                                   |
| `pa.int16()`, `pa.uint8()`                                                | `short`                                  |
| `pa.uint16()`                                                             | `char`                                   |
| `pa.int32()`                                                              | `int`                                    |
| `pa.int64()`, `pa.uint32()`                                               | `long`                                   |
| `pa.uint64()`                                                             | `java.math.BigInteger`                   |
| `pa.float16()`, `pa.float32()`                                            | `float`                                  |
| `pa.float64()`                                                            | `double`                                 |
| `pa.time32("s")`, `pa.time32("ms")`, `pa.time64("us")`, `pa.time64("ns")` | `java.time.LocalTime`                    |
| `pa.timestamp(unit)` with no time zone, for any unit                      | `java.time.Instant`                      |
| `pa.date32()`, `pa.date64()`                                              | `java.time.LocalDate`                    |
| `pa.duration(unit)`, for any unit                                         | `java.time.Duration`                     |
| `pa.month_day_nano_interval()`                                            | `org.apache.arrow.vector.PeriodDuration` |
| `pa.binary()`                                                             | `byte[]`                                 |
| `pa.string()`, `pa.utf8()`                                                | `java.lang.String`                       |

The parameterized types map to these Deephaven column types:

| PyArrow type                                                                                            | Deephaven column type                                                                |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `pa.decimal128(precision, scale)`, `pa.decimal256(precision, scale)`                                    | `java.math.BigDecimal`                                                               |
| `pa.list_(value_type)`, `pa.list_view(value_type)`, `pa.list_(value_type, list_size)` (fixed-size list) | An array of the element's Deephaven type, such as `int[]` for `pa.list_(pa.int32())` |
| `pa.map_(key_type, item_type)`                                                                          | `java.util.Map`                                                                      |
| `pa.binary(length)` (fixed-size binary)                                                                 | `byte[]`                                                                             |
| `pa.dense_union(...)`, `pa.sparse_union(...)`                                                           | `java.lang.Object`                                                                   |
| `pa.timestamp(unit, tz="UTC")`                                                                          | `java.time.Instant`                                                                  |
| `pa.timestamp(unit, tz=...)` with any other time zone string, including `"Etc/UTC"` and `"+00:00"`      | `java.time.ZonedDateTime`                                                            |

`to_table` raises a `DHError` for any other column type, including large strings, large binary, large lists, and structs.

A round trip through [`to_arrow`](./to-arrow.md) and `to_table` does not restore every column type. For example, `java.math.BigDecimal` and `java.math.BigInteger` columns come back as `byte[]`, `java.time.ZonedDateTime` columns come back as `java.time.Instant`, and columns of most other object types come back as `java.lang.String`.

## Examples

The following example converts a PyArrow table to a Deephaven table:

```python order=source
from deephaven import arrow as dharrow
import pyarrow as pa

n_legs = pa.array([2, 4, 5, 100])
animals = pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
sizes = pa.array(["Medium", "Big", "Small", "Small"])
source_pa = pa.table({"NumLegs": n_legs, "Animals": animals, "Sizes": sizes})

source = dharrow.to_table(pa_table=source_pa)
```

The following example converts only the `Animals` and `NumLegs` columns of a PyArrow table to a Deephaven table. The result lists `Animals` first because `cols` lists it first.

```python order=source
from deephaven import arrow as dharrow
import pyarrow as pa

n_legs = pa.array([2, 4, 5, 100])
animals = pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
sizes = pa.array(["Medium", "Big", "Small", "Small"])
source_pa = pa.table({"NumLegs": n_legs, "Animals": animals, "Sizes": sizes})

source = dharrow.to_table(pa_table=source_pa, cols=["Animals", "NumLegs"])
```

## Related documentation

- [Deephaven and PyArrow](../../how-to-guides/pyarrow.md)
- [`read_feather`](./read-feather.md)
- [`to_arrow`](./to-arrow.md)
- [Pydoc](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.to_table)
