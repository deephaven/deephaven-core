---
title: Data types in the Deephaven Python dtypes package
sidebar_label: Data types
---

Deephaven defines its Python types in the `deephaven.dtypes` package.

## Importing `deephaven.dtypes`

Deephaven recommends importing this package as follows:

```
import deephaven.dtypes as dht
```

## Values

The following values can be found in the `deephaven.dtypes` package:

<!--FIXME: remove types that were eliminated by new time lib-->

```shell
bool_           # Java boolean
byte            # Signed byte
int8            # Signed byte
short           # Signed short
int16           # Signed short
char            # Character
int32           # Signed 32 bit integer
long            # Signed 64 bit integer
int64           # Signed 64 bit integer
float32         # Single precision floating point
single          # Single precision floating point
float64         # Double precision floating point
double          # Double precision floating point
string          # String
BigDecimal      # Java BigDecimal
StringSet       # Deephaven StringSet
Instant         # Java Instant
LocalDate       # Java LocalDate
LocalTime       # Java LocalTime
ZonedDateTime   # Java ZonedDateTime
Duration        # Java Duration
Period          # Java Period
TimeZone        # Java TimeZone
PyObject        # Python object
JObject         # Java object
byte_array      # Byte array
int8_array      # Byte array
short_array     # Short array
int16_array     # Short array
int32_array     # 32 bit integer array
long_array      # 64 bit integer array
int64_array     # 64 bit integer array
single_array    # Single precision floating point array
float32_array   # Single precision floating point array
double_array    # Double precision floating point array
float64_array   # Double precision floating point array
string_array    # String array
instant_array   # Instant array
zdt_array       # ZonedDateTime array
```

## Example usage

One application of Deephaven types is to set the column types for a [Dynamic Table Writer](../../how-to-guides/table-publisher.md#dynamictablewriter). The following example creates 5 different tables, each containing a column for a different type contained within `deephaven.dtypes`.

```python order=t1,t2,t3,t4,t5
from deephaven import DynamicTableWriter
from deephaven import dtypes as dht
from deephaven import time as dhtu

col_defs_1 = {
    "Boolean": dht.bool_,
    "Byte": dht.byte,
    "Short": dht.short,
    "Int32": dht.int32,
    "Int64": dht.long,
}
col_defs_2 = {
    "Single": dht.single,
    "Double": dht.double,
    "String": dht.string,
    "BigDecimal": dht.BigDecimal,
}
col_defs_3 = {
    "DateTime": dht.Instant,
    "Period": dht.Period,
    "PyObject": dht.PyObject,
    "JObject": dht.JObject,
}
col_defs_4 = {
    "ByteArray": dht.byte_array,
    "ShortArray": dht.short_array,
    "IntArray": dht.int32_array,
    "LongArray": dht.long_array,
}
col_defs_5 = {
    "SingleArray": dht.single_array,
    "DoubleArray": dht.double_array,
    "StringArray": dht.string_array,
    "InstantArray": dht.instant_array,
}

dtw1 = DynamicTableWriter(col_defs_1)
t1 = dtw1.table
dtw1.write_row(True, 1, 2, 3, 4)

dtw2 = DynamicTableWriter(col_defs_2)
t2 = dtw2.table
BigDecimal = dht.BigDecimal
dtw2.write_row(3.1, 2.9, "Hello World!", BigDecimal(3.14159))

dtw3 = DynamicTableWriter(col_defs_3)
t3 = dtw3.table
dtw3.write_row(dhtu.dh_now(), dhtu.to_j_period("P1W"), 3, "a")

dtw4 = DynamicTableWriter(col_defs_4)
t4 = dtw4.table
dtw4.write_row(
    dht.array(dht.byte, [1, 2, 3]),
    dht.array(dht.short, [1, 2, 3]),
    dht.array(dht.int32, [1, 2, 3]),
    dht.array(dht.long, [1, 2, 3]),
)

dtw5 = DynamicTableWriter(col_defs_5)
t5 = dtw5.table
dtw5.write_row(
    dht.array(dht.single, [1.1, 2.2]),
    dht.array(dht.double, [1.1, 2.2]),
    dht.array(dht.string, ["a", "b"]),
    dht.array(
        dht.Instant,
        [
            dhtu.to_j_instant("2021-01-01T00:00:00 ET"),
            dhtu.to_j_instant("2022-01-01T00:00:00 ET"),
        ],
    ),
)
```

One application of Deephaven types is to set the column types for a [Dynamic Table Writer](../../how-to-guides/table-publisher.md#dynamictablewriter). The following example creates 4 different tables, each containing a column for a different type contained within `deephaven.dtypes`.

```python order=t1,t2,t3,t4
from deephaven import DynamicTableWriter
from deephaven import dtypes as dht
from deephaven import time as dhtu

col_defs_1 = {
    "Boolean": dht.bool_,
    "Byte": dht.byte,
    "Short": dht.short,
    "Int32": dht.int32,
    "Int64": dht.long,
}
col_defs_2 = {
    "Single": dht.single,
    "Double": dht.double,
    "String": dht.string,
    "BigDecimal": dht.BigDecimal,
}
col_defs_3 = {
    "ByteArray": dht.byte_array,
    "ShortArray": dht.short_array,
    "IntArray": dht.int32_array,
    "LongArray": dht.long_array,
}
col_defs_4 = {
    "SingleArray": dht.single_array,
    "DoubleArray": dht.double_array,
    "StringArray": dht.string_array,
    "InstantArray": dht.instant_array,
}

dtw1 = DynamicTableWriter(col_defs_1)
t1 = dtw1.table
dtw1.write_row(True, 1, 2, 3, 4)

dtw2 = DynamicTableWriter(col_defs_2)
t2 = dtw2.table
BigDecimal = dht.BigDecimal
dtw2.write_row(3.1, 2.9, "Hello World!", BigDecimal(3.14159))

dtw3 = DynamicTableWriter(col_defs_3)
t3 = dtw3.table
dtw3.write_row(
    dht.array(dht.byte, [1, 2, 3]),
    dht.array(dht.short, [1, 2, 3]),
    dht.array(dht.int32, [1, 2, 3]),
    dht.array(dht.long, [1, 2, 3]),
)

dtw4 = DynamicTableWriter(col_defs_4)
t4 = dtw4.table
dtw4.write_row(
    dht.array(dht.single, [1.1, 2.2]),
    dht.array(dht.double, [1.1, 2.2]),
    dht.array(dht.string, ["a", "b"]),
    dht.array(
        dht.Instant,
        [
            dhtu.to_j_instant("2021-01-01T00:00:00 ET"),
            dhtu.to_j_instant("2022-01-01T00:00:00 ET"),
        ],
    ),
)
```

## Related documentation

- [How to write data to an in-memory, real-time table](../../how-to-guides/table-publisher.md#dynamictablewriter)
