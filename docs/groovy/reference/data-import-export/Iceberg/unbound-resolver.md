---
title: UnboundResolver
sidebar_label: UnboundResolver
---

The `UnboundResolver` class provides a consolidated set of inference options for use in [`LoadTableOptions`](./load-table-options.md). It's most useful when the caller knows the definition of the table they want to load, and can provide an explicit mapping of Iceberg columns to Deephaven columns.

## Constructors

An `UnboundResolver` can be constructed using its [builder](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.Builder.html):

```groovy syntax
import io.deephaven.iceberg.util.UnboundResolver

resolver = UnboundResolver.builder()
    .definition(definition)
    .putAllColumnInstructions(entries)
    .putColumnInstructions(key, value)
    .schema(schema)
    .build()
```

- [`definition`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.Builder.html#definition(io.deephaven.engine.table.TableDefinition)): Set the Deephaven table definition to use when loading the Iceberg table.
- [`putAllColumnInstructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.Builder.html#putAllColumnInstructions(java.util.Map)): Add all specified keys and their corresponding [`ColumnInstructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/ColumnInstructions.html) to the resolver.
- [`putColumnInstructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.Builder.html#putColumnInstructions(java.lang.String,io.deephaven.iceberg.util.ColumnInstructions)): Add a single key and its corresponding [`ColumnInstructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/ColumnInstructions.html) to the resolver.
- [`schema`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.Builder.html#schema(io.deephaven.iceberg.util.SchemaProvider)): Set the [`SchemaProvider`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/SchemaProvider.html) for the resolver.

## Methods

- [`columnInstructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.html#columnInstructions()): The mapping of Deephaven table columns to Iceberg table columns.
- [`definition`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.html#definition()): The Deephaven table definition.
- [`schema`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.html#schema()): The schema to use for inference. The default is [`SchemaProvider.fromCurrent`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/SchemaProvider.html#fromCurrent()).
- [`walk`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.html#walk(io.deephaven.iceberg.util.ResolverProvider.Visitor)): Walk the resolver with the provided visitor.

## Example

The following example constructs an `UnboundResolver` for the `nyc.taxis` table with schema specifications that map Iceberg column names to table column names that align with Deephaven's recommended naming conventions. Additionally, because the resolver maps columns to field IDs, it makes the integration more robust against Iceberg schema changes.

```groovy order=null
import io.deephaven.iceberg.util.ColumnInstructions
import io.deephaven.iceberg.util.UnboundResolver
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

taxisDef = TableDefinition.of(
    ColumnDefinition.ofLong("VendorID"),
    ColumnDefinition.ofTime("PickupTime"),
    ColumnDefinition.ofTime("DropoffTime"),
    ColumnDefinition.ofDouble("NumPassengers"),
    ColumnDefinition.ofDouble("TripDistance"),
    ColumnDefinition.ofDouble("RateCodeID"),
    ColumnDefinition.ofString("StoreAndFwdFlag"),
    ColumnDefinition.ofLong("PickupLocationID"),
    ColumnDefinition.ofLong("DropoffLocationID"),
    ColumnDefinition.ofLong("PaymentType"),
    ColumnDefinition.ofDouble("FareAmount"),
    ColumnDefinition.ofDouble("Extra"),
    ColumnDefinition.ofDouble("MtaTax"),
    ColumnDefinition.ofDouble("Tip"),
    ColumnDefinition.ofDouble("Tolls"),
    ColumnDefinition.ofDouble("ImprovementSurcharge"),
    ColumnDefinition.ofDouble("TotalCost"),
    ColumnDefinition.ofDouble("CongestionSurcharge"),
    ColumnDefinition.ofDouble("AirportFee")
)
resolver = UnboundResolver.builder()
    .definition(taxisDef)
    .putColumnInstructions("VendorID", ColumnInstructions.schemaField(1))
    .putColumnInstructions("PickupTime", ColumnInstructions.schemaField(2))
    .putColumnInstructions("DropoffTime", ColumnInstructions.schemaField(3))
    .putColumnInstructions("NumPassengers", ColumnInstructions.schemaField(4))
    .putColumnInstructions("TripDistance", ColumnInstructions.schemaField(5))
    .putColumnInstructions("RateCodeID", ColumnInstructions.schemaField(6))
    .putColumnInstructions("StoreAndFwdFlag", ColumnInstructions.schemaField(7))
    .putColumnInstructions("PickupLocationID", ColumnInstructions.schemaField(8))
    .putColumnInstructions("DropoffLocationID", ColumnInstructions.schemaField(9))
    .putColumnInstructions("PaymentType", ColumnInstructions.schemaField(10))
    .putColumnInstructions("FareAmount", ColumnInstructions.schemaField(11))
    .putColumnInstructions("Extra", ColumnInstructions.schemaField(12))
    .putColumnInstructions("MtaTax", ColumnInstructions.schemaField(13))
    .putColumnInstructions("Tip", ColumnInstructions.schemaField(14))
    .putColumnInstructions("Tolls", ColumnInstructions.schemaField(15))
    .putColumnInstructions("ImprovementSurcharge", ColumnInstructions.schemaField(16))
    .putColumnInstructions("TotalCost", ColumnInstructions.schemaField(17))
    .putColumnInstructions("CongestionSurcharge", ColumnInstructions.schemaField(18))
    .putColumnInstructions("AirportFee", ColumnInstructions.schemaField(19))
    .build()
```

The resolver can then be passed into a [`LoadTableOptions`](./load-table-options.md) object when loading an Iceberg table into Deephaven.

## Related documentation

- [`InferenceResolver`](./inference-resolver.md)
- [`LoadTableOptions`](./load-table-options.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/UnboundResolver.html)
