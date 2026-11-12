---
id: custom-data-sources
title: Can I integrate custom data sources with Deephaven?
---

Yes, you can integrate custom data sources with Deephaven. Deephaven reads common formats such as Parquet, CSV, and Kafka out of the box, and you can also integrate your own data stores to use Deephaven's engine, analytics, and visualization capabilities.

There are three main integration approaches:

- **Static in-memory tables** — Similar to CSV and JDBC imports.
- **Dynamic in-memory tables** — For real-time data feeds like multicast distribution systems.
- **Lazily-loaded on-disk tables** — For large datasets like Apache Parquet files.

## Understanding Deephaven table structure

Each Deephaven table consists of:

- A [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) — An ordered set of long keys representing valid row addresses.
- Named [`ColumnSources`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) — A Java map from column name to `ColumnSource`, which acts as a dictionary from row key to cell value.

To construct a table for use by Deephaven engine operations, create a [`QueryTable`](/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) by passing in a [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) and `Map<String, ColumnSource>`. Use a [`LinkedHashMap`](/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) to preserve column order.

## Static in-memory tables

For simple static data sources, create a table with a flat address space where the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) includes keys 0 to size-1.

Here's an example of creating a static table from custom data:

```groovy order=customTable
import io.deephaven.engine.rowset.RowSetFactory
import io.deephaven.engine.rowset.TrackingRowSet
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource
import io.deephaven.engine.table.impl.QueryTable

// Your custom data as arrays
String[] symbols = ["AAPL", "GOOGL", "MSFT", "AMZN"] as String[]
double[] prices = [150.25, 2800.50, 380.75, 3400.00] as double[]
int[] volumes = [1000000, 500000, 750000, 600000] as int[]

// Create a TrackingRowSet for indices 0 to size-1
TrackingRowSet rowSet = RowSetFactory.flat(symbols.length).toTracking()

// Create column sources from arrays
columnSources = new LinkedHashMap<>()
columnSources.put("Symbol", ArrayBackedColumnSource.getMemoryColumnSource(symbols, String.class, null))
columnSources.put("Price", ArrayBackedColumnSource.getMemoryColumnSource(prices))
columnSources.put("Volume", ArrayBackedColumnSource.getMemoryColumnSource(volumes))

// Create the QueryTable
customTable = new QueryTable(rowSet, columnSources)
```

The [`ArrayBackedColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/sources/ArrayBackedColumnSource.html) automatically determines the column type for primitive arrays (e.g., `int[]`, `double[]`). For object arrays (e.g., `String[]`), you must explicitly specify the type using `getMemoryColumnSource(array, Type.class, null)`.

## Dynamic in-memory tables

Dynamic tables allow you to integrate real-time data feeds. These tables change on each Deephaven [update cycle](../../conceptual/table-update-model.md), a periodic pass (once per second by default) in which the engine applies new data and notifies downstream operations of the changes.

For most use cases, [`TablePublisher`](../table-operations/create/TablePublisher.md) is the recommended way to create dynamic tables. It produces a [blink table](../../conceptual/table-types.md#specialization-3-blink) and exposes an `add` method for publishing data. See the [how-to guide](../../how-to-guides/table-publisher.md) for full details.

```groovy order=null
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher
import static io.deephaven.engine.util.TableTools.*

definition = TableDefinition.of(
    ColumnDefinition.ofString("Symbol"),
    ColumnDefinition.ofDouble("Price"),
)

publisher = TablePublisher.of("Price feed", definition, null, null)
blinkTable = publisher.table()

// Publish data by calling publisher.add(tableWithData)
publisher.add(newTable(stringCol("Symbol", "AAPL"), doubleCol("Price", 150.25)))
```

### Advanced: raw QueryTable approach

For cases that require direct control over column sources — such as integrating a custom data store or implementing a lazy-loading [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) — you can build a dynamic table manually using [`QueryTable`](/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) and `notifyListeners`. This approach is significantly more complex than [`TablePublisher`](../table-operations/create/TablePublisher.md), and most use cases do not need it.

```groovy order=dynamicTable
import io.deephaven.engine.rowset.RowSetFactory
import io.deephaven.engine.rowset.TrackingRowSet
import io.deephaven.engine.table.impl.TableUpdateImpl
import io.deephaven.engine.rowset.RowSetShiftData
import io.deephaven.engine.table.ModifiedColumnSet
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource
import io.deephaven.engine.table.impl.QueryTable

// Initialize data arrays
int size = 100
String[] symbols = new String[size]
double[] prices = new double[size]

// Populate initial data
for (int i = 0; i < size; i++) {
    symbols[i] = "SYM" + i
    prices[i] = 100.0 + i
}

// Create column sources
symbolSource = ArrayBackedColumnSource.getMemoryColumnSource(symbols, String.class, null)
priceSource = ArrayBackedColumnSource.getMemoryColumnSource(prices)

// Enable previous value tracking for incremental updates
priceSource.startTrackingPrevValues()

// Create the table
TrackingRowSet rowSet = RowSetFactory.flat(size).toTracking()
columnSources = new LinkedHashMap<>()
columnSources.put("Symbol", symbolSource)
columnSources.put("Price", priceSource)

dynamicTable = new QueryTable(rowSet, columnSources)

// Make the table refreshing so it can receive updates
dynamicTable.setRefreshing(true)

// To update the table, use a time table to trigger updates on each cycle
import static io.deephaven.engine.util.TableTools.timeTable

// Create a time table that ticks every second to drive updates
updateTrigger = timeTable("PT1S")

// Listen to the trigger table and update dynamicTable on each tick.
// The listener must be stored in a variable so it is not garbage-collected.
updater = new io.deephaven.engine.table.impl.InstrumentedTableUpdateListener("DynamicTableUpdater") {
    @Override
    public void onUpdate(io.deephaven.engine.table.TableUpdate upstream) {
        // Example update logic: Modify prices for rows 0-9
        RowSet modifiedRows = RowSetFactory.fromRange(0, 9)
        
        // Update the column source values
        for (long i = 0; i < 10; i++) {
            double newPrice = prices[(int)i] + Math.random() * 10.0
            priceSource.set(i, newPrice)
        }
        
        // Create a TableUpdate describing the changes
        TableUpdateImpl update = new TableUpdateImpl(
            RowSetFactory.empty(),  // added rows
            RowSetFactory.empty(),  // removed rows
            modifiedRows,           // modified rows
            RowSetShiftData.EMPTY,  // row shifts
            ModifiedColumnSet.ALL   // modified columns (all columns marked as modified)
        )
        
        // Notify listeners of the update
        dynamicTable.notifyListeners(update)
    }
    
    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        originalException.printStackTrace()
    }
}

updateTrigger.addUpdateListener(updater)

// Register the listener as a parent so the engine knows dynamicTable depends on it
dynamicTable.addParentReference(updater)
```

## On-disk tables with lazy loading

For large datasets, implement custom [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) classes that load data on demand rather than loading it all at once.

Key implementation points:

- Implement `get` and the type-specific getters your column needs (`getBoolean`, `getByte`, `getChar`, `getShort`, `getInt`, `getLong`, `getFloat`, `getDouble`).
- Implement `fillChunk` for efficient bulk data fetching.
- Cache data to avoid repeated disk reads.
- For immutable on-disk data, return current values from the previous-value methods (`getPrev`, `getPrevDouble`, and so on). Every column source must report the value a cell had at the start of the current update cycle, and immutable data never changes, so the two are always equal. See [Removed or modified values](../../conceptual/table-update-model.md#removed-or-modified-values).

Here's a conceptual example:

> [!WARNING]
> This is an **incomplete skeleton** for illustration purposes only and **will not compile or run**. A complete `ColumnSource` implementation requires implementing ~30+ methods from the interface. For production use, extend [`AbstractColumnSource`](/core/javadoc/io/deephaven/engine/table/impl/AbstractColumnSource.html) which provides default implementations for most methods.

```groovy skip-test
import io.deephaven.engine.table.ColumnSource
import io.deephaven.engine.table.ChunkSource
import io.deephaven.engine.table.ChunkSource.FillContext
import io.deephaven.engine.rowset.RowSequence
import io.deephaven.chunk.WritableChunk
import io.deephaven.chunk.attributes.Values

class CustomFileColumnSource implements ColumnSource<Double> {
    private final File dataFile
    private final Map<Long, Double> cache = new HashMap<>()
    
    CustomFileColumnSource(File dataFile) {
        this.dataFile = dataFile
    }
    
    @Override
    Double get(long rowKey) {
        // Check cache first
        if (cache.containsKey(rowKey)) {
            return cache.get(rowKey)
        }
        
        // Read from disk and cache
        double value = readValueFromDisk(rowKey)
        cache.put(rowKey, value)
        return value
    }
    
    @Override
    void fillChunk(FillContext context, WritableChunk<? super Values> destination, RowSequence rowSequence) {
        // Efficiently read multiple values at once
        // Implementation depends on your file format
    }
    
    private double readValueFromDisk(long rowKey) {
        // Your custom logic to read from disk
        // This is where you'd implement file format-specific reading
    }
}
```

### Coalesced vs. uncoalesced tables

Deephaven tables can be "coalesced" or "uncoalesced":

- **Uncoalesced tables** — Reference data but cannot be used for most query operations.
- **Coalesced tables** — Fully formed `QueryTable` instances ready for query operations.

Tables are automatically coalesced when needed. For example, when reading from disk with partitioning filters like `"Date=today()"`, the table remains uncoalesced until the filter is applied, avoiding the need to determine the complete `RowSet` upfront.

### Data indexes for performance

Tables can carry data indexes (similar to database indexes) that map key values to the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) of matching rows, which speeds up filters, joins, and aggregations. See [Data indexes](../../how-to-guides/data-indexes.md).

Deephaven's Apache Parquet integration combines lazy loading, caching, and data indexes:

- Partition discovery before table coalescing.
- Buffer caching to avoid re-reading data.
- Data indexes stored alongside the Parquet files.

## Related documentation

- [Deephaven Core Javadoc](https://deephaven.io/core/javadoc/)
- [Iceberg](../../how-to-guides/data-import-export/iceberg.md)
- [Parquet](../../how-to-guides/data-import-export/parquet-export.md)
- [Write data to a real-time table](../../how-to-guides/table-publisher.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
