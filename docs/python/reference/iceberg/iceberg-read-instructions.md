---
title: IcebergReadInstructions
---

The `IcebergReadInstructions` class specifies the instructions for reading Iceberg tables into Deephaven. These include column renames, table definitions, and special instructions for loading files from cloud storage.

## Syntax

```python syntax
IcebergReadInstructions(
    table_definition: Union[Dict[str, DType], List[Column]] = None,
    data_instructions: S3Instructions = None,
    column_renames: Dict[str, str] = None,
    update_mode: IcebergUpdateMode = None,
    snapshot_id: int = None,
    ignore_resolving_errors: bool = False,
    pruning_expression: jpy.JType = None
)
```

## Parameters

<ParamTable>
<Param name="table_definition" type="Union[Dict[str, DType], List[Column]]" Optional>

The table definition. If not given, the definition is inferred from the Iceberg schema. Setting a definition guarantees the returned table has the given definition. This is mostly used to specify a subset of Iceberg schema columns.

</Param>
<Param name="data_instructions" type="S3Instructions" Optional>

Special instructions for reading data files from S3 cloud storage.

</Param>
<Param name="column_renames" type="Dict[str, str]" Optional>

A mapping of old to new column names for the table. If not given, the column names are the same as the Iceberg schema.

</Param>
<Param name="update_mode" type="IcebergUpdateMode" Optional>

The update mode for the table. Options include:

- `IcebergUpdateMode.static()`: Specifies that the Iceberg table should be loaded once and not refreshed.
- `IcebergUpdateMode.manual_refresh()`: Specifies that the Iceberg table should be loaded once and refreshed manually.
- `IcebergUpdateMode.auto_refresh()`: Specifies that the Iceberg table should be loaded once and refreshed automatically. The default refresh interval is 60 seconds, but can be changed with the `auto_refresh_ms` input parameter.

</Param>
<Param name="snapshot_id" type="int" Optional>

The snapshot ID to read. If not given, the most recent snapshot ID is used.

</Param>
<Param name="ignore_resolving_errors" type="bool" Optional>

Controls whether to ignore unexpected resolving errors by silently returning `NULL` data for columns that cannot be resolved in the data files where they should be present. Such errors may indicate an incorrect resolver or name mapping, or an Iceberg metadata or data issue. The default is `False`.

</Param>
<Param name="pruning_expression" type="jpy.JType" Optional>

An [`org.apache.iceberg.expressions.Expression`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/expressions/Expression.html) that skips Iceberg data files that cannot contain matching rows. Field names resolve against the Iceberg schema rather than against Deephaven column names. The default is `Expressions.alwaysTrue`, which prunes nothing.

This parameter prunes; it does not filter. Iceberg prunes using partition values and data file statistics, so a surviving data file is read in full and the result is a superset of the rows that satisfy the expression. To obtain exactly those rows, apply an equivalent Deephaven filter to the result.

> [!IMPORTANT]
> Pruning on a non-partition field relies on the per-column value bounds that Iceberg records for each data file. Deephaven's Iceberg writer does not record those statistics, so an expression over a non-partition field prunes nothing on a table that Deephaven wrote. Pruning on a partition field applies in all cases, because Iceberg records partition values regardless of statistics.

</Param>
</ParamTable>

## Methods

None.

## Constructors

An `IcebergReadInstructions` is constructed directly from the class.

## Examples

The following example creates an `IcebergReadInstructions` object that renames Iceberg columns `region` and `item_type` to `Area` and `Category` in Deephaven, respectively:

```python order=null
from deephaven.experimental import iceberg

custom_instructions = iceberg.IcebergReadInstructions(
    column_renames={"region": "Area", "item_type": "Category"}
)
```

The following example creates an `IcebergReadInstructions` object that renames columns as well as specifies the table definition:

```python order=null
from deephaven.experimental import iceberg
from deephaven import dtypes as dht

custom_instructions = iceberg.IcebergReadInstructions(
    column_renames={"region": "Area", "item_type": "Category", "unit_price": "Price"},
    table_definition={
        "Area": dht.string,
        "Category": dht.string,
        "Price": dht.double,
    },
)
```

The following example creates four `IcebergReadInstructions` objects. The first is for static Iceberg tables, the second is for Iceberg tables that can be manually refreshed, and the third and fourth are for Iceberg tables that will be refreshed automatically. The third uses the default value of 60 seconds, whereas the fourth sets the interval to 30 seconds.

```python order=null
from deephaven.experimental import iceberg

static_mode = iceberg.IcebergUpdateMode.static()
manual_refresh_mode = iceberg.IcebergUpdateMode.manual_refresh()
auto_refresh_mode_60s = iceberg.IcebergUpdateMode.auto_refresh()
auto_refresh_mode_30s = iceberg.IcebergUpdateMode.auto_refresh(auto_refresh_ms=30000)

static_instructions = iceberg.IcebergReadInstructions(update_mode=static_mode)
manual_refresh_instructions = iceberg.IcebergReadInstructions(
    update_mode=manual_refresh_mode
)
auto_refresh_instructions_60s = iceberg.IcebergReadInstructions(
    update_mode=auto_refresh_mode_60s
)
auto_refresh_instructions_30s = iceberg.IcebergReadInstructions(
    update_mode=auto_refresh_mode_30s
)
```

The following example creates an `IcebergReadInstructions` object that tells a catalog adapter about the region, access information, and endpoint for reading Iceberg tables from S3 cloud storage:

```python docker-config=iceberg order=null
from deephaven.experimental import iceberg
from deephaven.experimental import s3

s3_instructions = s3.S3Instructions(
    region_name=aws_region,
    access_key_id=aws_access_key_id,
    secret_access_key=aws_secret_access_key,
    endpoint_override=s3_endpoint,
)

iceberg_instructions = iceberg.IcebergReadInstructions(
    data_instructions=s3_instructions
)
```

The following example creates an `IcebergReadInstructions` object that prunes data files whose `region` partition cannot contain the value `EMEA`. Because pruning is not filtering, the query applies an equivalent Deephaven filter to the result to obtain exactly the matching rows:

```python order=null
import jpy
from deephaven.experimental import iceberg

Expressions = jpy.get_type("org.apache.iceberg.expressions.Expressions")

pruning_instructions = iceberg.IcebergReadInstructions(
    pruning_expression=Expressions.equal("region", "EMEA")
)

# emea_table = table_adapter.table(pruning_instructions).where("Region = `EMEA`")
```

Numeric and temporal literals require a typed [`Literal`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/expressions/Literal.html), because jpy narrows a Python `int` to a Java `Byte` or `Short`, both of which Iceberg rejects. The following example prunes on an integer field and on a timestamp field:

```python order=null
import jpy
from deephaven.experimental import iceberg

Expressions = jpy.get_type("org.apache.iceberg.expressions.Expressions")
Literal = jpy.get_type("org.apache.iceberg.expressions.Literal")
Operation = jpy.get_type("org.apache.iceberg.expressions.Expression$Operation")

# Literal.of accepts a primitive, so the integer width survives the call into Java.
year_instructions = iceberg.IcebergReadInstructions(
    pruning_expression=Expressions.predicate(Operation.GT, "year", Literal.of(2023))
)

# micros, millis, and nanos state the unit explicitly. Iceberg stores a timestamp column as
# microseconds from the epoch unless the column is timestamp_ns.
timestamp_instructions = iceberg.IcebergReadInstructions(
    pruning_expression=Expressions.predicate(
        Operation.GT_EQ, "pickup_time", Expressions.micros(1767225600000000)
    )
)
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergReadInstructions)
