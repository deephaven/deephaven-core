---
title: Table data service
---

This guide covers using the Table Data Service API to integrate custom, partitioned, lazily-loaded data into Deephaven workflows.

In Groovy, you use the Java [`TableDataService`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableDataService.html) interface and its implementations directly. The Python Table Data Service API is a higher-level wrapper built on top of the same interface.

> [!NOTE]
> This feature is currently experimental. The API and its characteristics are subject to change.

### `TableDataService` API

The [`TableDataService`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/locations/TableDataService.html) interface provides a way to integrate external data sources into Deephaven tables in both static and refreshing contexts. The data is partitioned, and each partition is loaded lazily — the engine only reads column data when a query needs it.

The API involves seven cooperating classes, each responsible for one layer of the data access stack:

| Class                                                                                                                             | Role                                                                     |
| --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| [`TableKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableKey.html)                                                | Unique identifier for a logical table                                    |
| [`TableLocationKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationKey.html)                                | Identifies a specific partition within a table                           |
| [`AbstractTableDataService`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableDataService.html)           | Service entry point: maps a `TableKey` to a `TableLocationProvider`      |
| [`AbstractTableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocationProvider.html) | Discovers and manages `TableLocation` objects for a given key            |
| [`AbstractTableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocation.html)                 | Per-partition: reports row count and creates `ColumnLocation` objects    |
| [`AbstractColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractColumnLocation.html)               | Per-column: provides typed data to the engine via `ColumnRegion` objects |
| [`PartitionAwareSourceTable`](/core/javadoc/io/deephaven/engine/table/impl/PartitionAwareSourceTable.html)                        | Assembles everything into a queryable Deephaven table                    |

The following sections describe the implementation requirements for each class and show a complete working example using an in-memory data store.

### `TableKey`

A [`TableKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableKey.html) is a unique identifier for a logical table. Implement [`ImmutableTableKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/ImmutableTableKey.html) (a sub-interface of `TableKey`) to get a default `makeImmutable()` that returns `this`. You must provide:

- `getImplementationName()`: A human-readable name used in logging.
- `append(LogOutput)`: Describes the key in log output.
- `hashCode()` and `equals()`: Must be consistent with each other.

```groovy test-set=1 order=null
import io.deephaven.base.log.LogOutput
import io.deephaven.engine.table.impl.locations.ImmutableTableKey

class StockPricesKey implements ImmutableTableKey {
    @Override
    String getImplementationName() { "StockPricesKey" }

    @Override
    LogOutput append(LogOutput logOutput) {
        logOutput.append(getImplementationName())
    }

    @Override
    int hashCode() { 0 }

    @Override
    boolean equals(Object other) { other instanceof StockPricesKey }

    @Override
    String toString() { getImplementationName() }
}
```

### `TableLocationKey`

A [`TableLocationKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationKey.html) identifies a specific partition within a table. Extend [`PartitionedTableLocationKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/PartitionedTableLocationKey.html), which handles partition-map storage, sorting, `hashCode()`, and `equals()`. Only `getImplementationName()` and `append(LogOutput)` need to be provided:

```groovy test-set=1 order=null
import io.deephaven.base.log.LogOutput
import io.deephaven.engine.table.impl.locations.impl.PartitionedTableLocationKey

class DateLocationKey extends PartitionedTableLocationKey {
    DateLocationKey(String date) {
        super(["Date": date])
    }

    @Override
    String getImplementationName() { "DateLocationKey" }

    @Override
    LogOutput append(LogOutput logOutput) {
        logOutput.append("DateLocationKey[Date=")
                 .append(partitions["Date"].toString())
                 .append("]")
    }
}
```

For unpartitioned tables, use the built-in singleton [`StandaloneTableLocationKey.getInstance()`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/StandaloneTableLocationKey.html).

### `ColumnLocation`

A [`ColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/ColumnLocation.html) provides the actual column data for one partition. It is created on demand by `AbstractTableLocation.makeColumnLocation()` and returns typed [`ColumnRegion`](/core/javadoc/io/deephaven/engine/table/impl/sources/regioned/ColumnRegion.html) objects that the engine uses to read data.

Extend [`AbstractColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractColumnLocation.html) and implement one `makeColumnRegion*` method per column type present in your table schema. For each, return an `AppendOnlyFixedSizePageRegion*` backed by an [`AppendOnlyRegionAccessor`](/core/javadoc/io/deephaven/generic/region/AppendOnlyRegionAccessor.html) that reads from your data store.

`AppendOnlyRegionAccessor` requires two methods:

- `readChunkPage(firstRowPosition, minimumSize, destination)`: Fills `destination` with at least `minimumSize` values starting at `firstRowPosition`.
- `size()`: Returns the total number of rows at this location.

The following example stores column data in typed arrays. Each array is keyed by column name in a map passed from `AbstractTableLocation`:

```groovy test-set=1 order=null
import io.deephaven.chunk.WritableChunk
import io.deephaven.chunk.attributes.Values
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.impl.locations.TableLocation
import io.deephaven.engine.table.impl.locations.impl.AbstractColumnLocation
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionByte
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionChar
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionDouble
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionFloat
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionInt
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionLong
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionObject
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionShort
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionDouble
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionLong
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionObject
import io.deephaven.generic.region.AppendOnlyRegionAccessor

class StockColumnLocation extends AbstractColumnLocation {
    private static final int PAGE_SIZE = 1 << 16
    private static final long REGION_MASK = RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK

    // Maps column name -> typed array (String[], double[], long[], etc.)
    private final Map<String, Object> columnArrays

    StockColumnLocation(TableLocation location, String name, Map<String, Object> columnArrays) {
        super(location, name)
        this.columnArrays = columnArrays
    }

    @Override
    boolean exists() {
        columnArrays.containsKey(getName())
    }

    @Override
    ColumnRegionDouble<Values> makeColumnRegionDouble(ColumnDefinition<?> columnDefinition) {
        double[] data = (double[]) columnArrays[getName()]
        new AppendOnlyFixedSizePageRegionDouble<>(REGION_MASK, PAGE_SIZE,
            new AppendOnlyRegionAccessor<Values>() {
                @Override
                void readChunkPage(long firstRowPosition, int minimumSize, WritableChunk<Values> destination) {
                    destination.setSize(0)
                    for (int i = 0; i < minimumSize; i++) {
                        destination.asWritableDoubleChunk().add(data[(int) (firstRowPosition + i)])
                    }
                }

                @Override
                long size() { data.length }
            })
    }

    @Override
    ColumnRegionLong<Values> makeColumnRegionLong(ColumnDefinition<?> columnDefinition) {
        long[] data = (long[]) columnArrays[getName()]
        new AppendOnlyFixedSizePageRegionLong<>(REGION_MASK, PAGE_SIZE,
            new AppendOnlyRegionAccessor<Values>() {
                @Override
                void readChunkPage(long firstRowPosition, int minimumSize, WritableChunk<Values> destination) {
                    destination.setSize(0)
                    for (int i = 0; i < minimumSize; i++) {
                        destination.asWritableLongChunk().add(data[(int) (firstRowPosition + i)])
                    }
                }

                @Override
                long size() { data.length }
            })
    }

    @Override
    ColumnRegionObject makeColumnRegionObject(ColumnDefinition columnDefinition) {
        Object[] data = (Object[]) columnArrays[getName()]
        new AppendOnlyFixedSizePageRegionObject<>(REGION_MASK, PAGE_SIZE,
            new AppendOnlyRegionAccessor<Values>() {
                @Override
                void readChunkPage(long firstRowPosition, int minimumSize, WritableChunk<Values> destination) {
                    destination.setSize(0)
                    for (int i = 0; i < minimumSize; i++) {
                        destination.asWritableObjectChunk().add(data[(int) (firstRowPosition + i)])
                    }
                }

                @Override
                long size() { data.length }
            })
    }

    // Throw for types not present in this example's schema
    @Override ColumnRegionChar<Values> makeColumnRegionChar(ColumnDefinition<?> d) { throw new UnsupportedOperationException() }
    @Override ColumnRegionByte<Values> makeColumnRegionByte(ColumnDefinition<?> d) { throw new UnsupportedOperationException() }
    @Override ColumnRegionShort<Values> makeColumnRegionShort(ColumnDefinition<?> d) { throw new UnsupportedOperationException() }
    @Override ColumnRegionInt<Values> makeColumnRegionInt(ColumnDefinition<?> d) { throw new UnsupportedOperationException() }
    @Override ColumnRegionFloat<Values> makeColumnRegionFloat(ColumnDefinition<?> d) { throw new UnsupportedOperationException() }
}
```

### `TableLocation`

A `TableLocation` represents one partition of a table. It reports the partition's row count to the engine and creates `ColumnLocation` objects on demand.

Extend `AbstractTableLocation` and implement:

- `makeColumnLocation(name)`: Returns a `ColumnLocation` for the named column.
- `refresh()`: Re-checks row count; call `handleUpdate(RowSet, timestamp)` to report the current size.
- `activateUnderlyingDataSource()`: Called on first subscriber; initialize subscription state and call `activationSuccessful`.
- `deactivateUnderlyingDataSource()`: Called when all subscribers detach; clear subscription state.
- `matchSubscriptionToken(token)`: Returns `true` if the token matches the current subscription.
- `getSortedColumns()`, `getDataIndexColumns()`, `hasDataIndex()`, `loadDataIndex()`: Return empty values if not supporting sorting or data indexes.

```groovy test-set=1 order=null
import io.deephaven.api.SortColumn
import io.deephaven.engine.rowset.RowSetFactory
import io.deephaven.engine.table.BasicDataIndex
import io.deephaven.engine.table.impl.locations.TableKey
import io.deephaven.engine.table.impl.locations.TableLocationKey
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation

class StockTableLocation extends AbstractTableLocation {
    private volatile Object subscriptionToken
    private final Map<String, Object> columnArrays
    private final int rowCount

    StockTableLocation(TableKey tableKey, TableLocationKey locationKey,
                       Map<String, Object> columnArrays) {
        super(tableKey, locationKey, true)
        this.columnArrays = columnArrays
        this.rowCount = java.lang.reflect.Array.getLength(columnArrays.values().first())
    }

    @Override
    protected StockColumnLocation makeColumnLocation(String name) {
        new StockColumnLocation(this, name, columnArrays)
    }

    @Override
    void refresh() {
        handleUpdate(RowSetFactory.flat(rowCount), System.currentTimeMillis())
    }

    @Override
    protected void activateUnderlyingDataSource() {
        def token = subscriptionToken = new Object()
        handleUpdate(RowSetFactory.flat(rowCount), System.currentTimeMillis())
        activationSuccessful(token)
    }

    @Override
    protected void deactivateUnderlyingDataSource() { subscriptionToken = null }

    @Override
    protected <T> boolean matchSubscriptionToken(T token) { token == subscriptionToken }

    // Groovy does not recognize Java interface default methods as concrete implementations,
    // so the DelegatingLivenessReferent defaults must be declared explicitly.
    @Override boolean tryRetainReference() { asLivenessReferent().tryRetainReference() }
    @Override void dropReference() { asLivenessReferent().dropReference() }
    @Override java.lang.ref.WeakReference getWeakReference() { new java.lang.ref.WeakReference<>(this) }

    @Override List<SortColumn> getSortedColumns() { [] }
    @Override List<String[]> getDataIndexColumns() { [] }
    @Override boolean hasDataIndex(String... columns) { false }
    @Override BasicDataIndex loadDataIndex(String... columns) { null }
    @Override String getImplementationName() { "StockTableLocation" }
}
```

### `TableLocationProvider`

A [`TableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationProvider.html) discovers and manages the set of [`TableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html) objects for a given [`TableKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableKey.html). Extend [`AbstractTableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocationProvider.html) and implement:

- `makeTableLocation(locationKey)`: Returns the [`TableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html) for the given [`TableLocationKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationKey.html).
- `refresh()`: Re-enumerates locations; call `handleTableLocationKeyAdded` for each known key.
- `activateUnderlyingDataSource()`: Subscribe and register known locations; call `activationSuccessful`.
- `deactivateUnderlyingDataSource()`: Unsubscribe.
- `matchSubscriptionToken(token)`: Returns `true` if token matches current subscription.

```groovy test-set=1 order=null
import io.deephaven.engine.table.impl.TableUpdateMode
import io.deephaven.engine.table.impl.locations.TableKey
import io.deephaven.engine.table.impl.locations.TableLocation
import io.deephaven.engine.table.impl.locations.TableLocationKey
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider

class StockTableLocationProvider extends AbstractTableLocationProvider {
    private volatile Object subscriptionToken
    private final Map<TableLocationKey, StockTableLocation> locations

    StockTableLocationProvider(TableKey tableKey,
                                Map<TableLocationKey, StockTableLocation> locations) {
        super(tableKey, true, TableUpdateMode.STATIC, TableUpdateMode.STATIC)
        this.locations = locations
    }

    @Override
    protected TableLocation makeTableLocation(TableLocationKey locationKey) {
        locations[locationKey]
    }

    @Override
    void refresh() {
        locations.keySet().each { handleTableLocationKeyAdded(it) }
    }

    @Override
    protected void activateUnderlyingDataSource() {
        def token = subscriptionToken = new Object()
        locations.keySet().each { handleTableLocationKeyAdded(it) }
        activationSuccessful(token)
    }

    @Override
    protected void deactivateUnderlyingDataSource() { subscriptionToken = null }

    @Override
    protected <T> boolean matchSubscriptionToken(T token) { token == subscriptionToken }

    @Override
    String getImplementationName() { "StockTableLocationProvider" }
}
```

### `AbstractTableDataService`

[`AbstractTableDataService`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableDataService.html) is the service entry point. It caches [`TableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationProvider.html) instances and only requires implementing `makeTableLocationProvider(TableKey)`:

```groovy test-set=1 order=null
import io.deephaven.engine.table.impl.locations.TableKey
import io.deephaven.engine.table.impl.locations.TableLocationProvider
import io.deephaven.engine.table.impl.locations.impl.AbstractTableDataService

class StockDataService extends AbstractTableDataService {
    // partitionData: date string -> (column name -> typed array)
    private final Map<String, Map<String, Object>> partitionData

    StockDataService(Map<String, Map<String, Object>> partitionData) {
        super("StockDataService")
        this.partitionData = partitionData
    }

    @Override
    protected TableLocationProvider makeTableLocationProvider(TableKey tableKey) {
        def locations = [:] as LinkedHashMap
        partitionData.each { date, columnArrays ->
            def locationKey = new DateLocationKey(date)
            locations[locationKey] = new StockTableLocation(tableKey, locationKey, columnArrays)
        }
        new StockTableLocationProvider(tableKey, locations)
    }
}
```

### Usage

The following code uses all six classes defined above to build a [`PartitionAwareSourceTable`](/core/javadoc/io/deephaven/engine/table/impl/PartitionAwareSourceTable.html) backed by an in-memory stock prices store with two date partitions:

```groovy test-set=1 order=stockPrices
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.PartitionAwareSourceTable
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl

// In-memory data: date partition -> (column name -> typed array)
def partitionData = [
    "2024-01-01": [
        "Symbol": ["AAPL", "GOOGL", "MSFT"] as String[],
        "Price":  [150.25d, 2800.50d, 380.75d] as double[],
        "Volume": [1_000_000L, 500_000L, 750_000L] as long[]
    ],
    "2024-01-02": [
        "Symbol": ["AAPL", "GOOGL", "MSFT"] as String[],
        "Price":  [152.75d, 2810.00d, 382.25d] as double[],
        "Volume": [1_200_000L, 600_000L, 800_000L] as long[]
    ]
]

// Date is a partitioning column — its values come from DateLocationKey, not from column data
def tableDefinition = TableDefinition.of(
    ColumnDefinition.ofString("Date").withPartitioning(),
    ColumnDefinition.ofString("Symbol"),
    ColumnDefinition.ofDouble("Price"),
    ColumnDefinition.ofLong("Volume")
)

def service = new StockDataService(partitionData)
def tableKey = new StockPricesKey()
def provider = service.getTableLocationProvider(tableKey)

// null updateSourceRegistrar = static (non-refreshing) table
stockPrices = new PartitionAwareSourceTable(
    tableDefinition,
    "StockPrices",
    RegionedTableComponentFactoryImpl.INSTANCE,
    provider,
    null
)
```

## Related documentation

- [`AbstractColumnLocation` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractColumnLocation.html)
- [`AbstractTableDataService` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableDataService.html)
- [`AbstractTableLocation` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocation.html)
- [`AbstractTableLocationProvider` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocationProvider.html)
- [`AppendOnlyRegionAccessor` Javadoc](/core/javadoc/io/deephaven/generic/region/AppendOnlyRegionAccessor.html)
- [`ColumnLocation` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/ColumnLocation.html)
- [`ColumnRegion` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/sources/regioned/ColumnRegion.html)
- [Custom data sources](../../reference/community-questions/custom-data-sources.md)
- [`ImmutableTableKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/ImmutableTableKey.html)
- [`PartitionAwareSourceTable` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/PartitionAwareSourceTable.html)
- [`PartitionedTableLocationKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/PartitionedTableLocationKey.html)
- [`StandaloneTableLocationKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/StandaloneTableLocationKey.html)
- [`TableDataService` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableDataService.html)
- [`TableKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableKey.html)
- [`TableLocation` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html)
- [`TableLocationKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationKey.html)
- [`TableLocationProvider` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationProvider.html)
