---
id: custom-data-sources
title: Can I integrate custom data sources with Deephaven?
sidebar_label: Can I integrate custom data sources?
---

Yes, you can integrate custom data sources with Deephaven. While Deephaven includes a proprietary columnar store for persistent historical and intraday data, you can integrate your own data stores to leverage Deephaven's efficient engine, analytics, and data visualization capabilities.

There are three main integration approaches:

- **Static in-memory tables** - Similar to CSV and JDBC imports.
- **Dynamic in-memory tables** - For real-time data feeds like multicast distribution systems.
- **Lazily-loaded on-disk tables** - For large datasets like Apache Parquet files.

## Understanding Deephaven table structure

Each Deephaven table consists of:

- A [`RowSet`](https://deephaven.io/core/javadoc/io/deephaven/engine/rowset/RowSet.html) - An ordered set of long keys representing valid row addresses.
- Named [`ColumnSources`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) - A Java map from column name to `ColumnSource`, which acts as a dictionary from row key to cell value.

To construct a table for use by Deephaven engine operations, create a [`QueryTable`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) by passing in a `RowSet` and `Map<String, ColumnSource>`. We recommend using a LinkedHashMap to preserve column order.

## Static in-memory tables

For simple static data sources, create a table with a flat address space where the `RowSet` includes keys 0 to size-1.

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

Dynamic tables allow you to integrate real-time data feeds. These tables update on each Deephaven update cycle and notify downstream operations of changes.

Key features of dynamic tables:

- Use the same `ArrayBackedColumnSource` as static tables.
- Poll for updates on each Deephaven update cycle.
- Track both current and previous values for incremental computation.

- Notify listeners using [`notifyListeners`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/BaseTable.html#notifyListeners(io.deephaven.engine.table.TableUpdate)).

Here's a simplified example of a dynamic table that updates periodically:

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

// Listen to the trigger table and update dynamicTable on each tick
updateTrigger.addUpdateListener(new io.deephaven.engine.table.impl.InstrumentedTableUpdateListener("DynamicTableUpdater") {
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
})
```

### Coalesced vs. uncoalesced tables

Deephaven tables can be "coalesced" or "uncoalesced":

- **Uncoalesced tables** - Reference data but cannot be used for most query operations.
- **Coalesced tables** - Fully formed `QueryTable` instances ready for query operations.

Tables are automatically coalesced when needed. For example, when reading from disk with partitioning filters like `"Date=today()"`, the table remains uncoalesced until the filter is applied, avoiding the need to determine the complete Index upfront.

## On-disk tables with lazy loading

For large datasets, implement custom [ColumnSource](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) classes that load data on demand rather than loading it all at once.

Key implementation points:

- Implement `get()` methods for each primitive type (`getByte`, `getShort`, `getInt`, `getLong`, `getChar`, `getFloat`, `getDouble`).
- Implement `fillChunk()` for efficient bulk data fetching.
- Cache data to avoid repeated disk reads.
- For immutable on-disk data, delegate previous-value calls to current-value calls.

Here's a conceptual example:

> [!WARNING]
> This is an **incomplete skeleton** for illustration purposes only and **will not compile or run**. A complete `ColumnSource` implementation requires implementing ~30+ methods from the interface. For production use, extend `AbstractColumnSource` which provides default implementations for most methods.

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

### Grouping information for performance

`ColumnSources` can provide grouping information (similar to database indexes) to optimize operations like filtering and joins. Grouping information is represented as a Map from a key to a `RowSet` of matching row keys.

Deephaven's Apache Parquet integration uses these techniques:

- Partition discovery before table coalescing.
- Buffer caching to avoid re-reading data.
- Grouping information stored on disk for efficient queries.

## Related documentation

- [Parquet](../../how-to-guides/data-import-export/parquet-export.md)
- [Iceberg](../../how-to-guides/data-import-export/iceberg.md)
- [Deephaven Core Javadoc](https://deephaven.io/core/javadoc/)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
