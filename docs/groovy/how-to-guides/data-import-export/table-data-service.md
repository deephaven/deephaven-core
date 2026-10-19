---
title: Table data service
---

This guide covers using the Table Data Service API to integrate custom, partitioned, lazily loaded data into Deephaven workflows.

In Groovy, you use the Java [`TableDataService`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableDataService.html) interface and its implementations directly. The Python Table Data Service API is a higher-level wrapper built on top of the same interface.

> [!NOTE]
> This feature is currently experimental. The API and its characteristics are subject to change.

## How the API fits together

The [`TableDataService`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableDataService.html) interface provides a way to integrate external data sources into Deephaven tables in both static and refreshing contexts. The example on this page builds both: a static table, and a refreshing table that updates as new data arrives. The data is partitioned, and the engine loads each partition lazily. It reads column data only when a query needs it.

The API involves seven cooperating types, each responsible for one layer of the data access stack. Each `Abstract*` class is a base implementation of the interface with the same name minus the prefix. For example, `AbstractTableLocation` implements [`TableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html).

| Type                                                                                                                              | Role                                                                                                                                                                 |
| --------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`TableKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableKey.html)                                                | Identifies a logical table                                                                                                                                           |
| [`TableLocationKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationKey.html)                                | Identifies one partition within a table                                                                                                                              |
| [`AbstractColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractColumnLocation.html)               | Provides one column's typed data for one partition through [`ColumnRegion`](/core/javadoc/io/deephaven/engine/table/impl/sources/regioned/ColumnRegion.html) objects |
| [`AbstractTableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocation.html)                 | Reports one partition's row count and creates [`ColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/ColumnLocation.html) objects                 |
| [`AbstractTableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocationProvider.html) | Discovers and manages [`TableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html) objects for a given key                           |
| [`AbstractTableDataService`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableDataService.html)           | Serves as the entry point and maps a `TableKey` to a [`TableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationProvider.html)    |
| [`PartitionAwareSourceTable`](/core/javadoc/io/deephaven/engine/table/impl/PartitionAwareSourceTable.html)                        | Assembles everything into a queryable Deephaven table                                                                                                                |

A static table calls the `refresh` method of the provider and of each location once to discover locations and row counts. A refreshing table subscribes instead when the provider and locations support subscriptions: the engine calls their `activateUnderlyingDataSource` methods, and they report new locations and rows as they arrive.

The example's provider and locations support both paths. The [`AbstractTableLocation`](#abstracttablelocation) and [`AbstractTableLocationProvider`](#abstracttablelocationprovider) sections describe the subscription methods, and [Example](#example) builds a static table and a refreshing table from the same provider.

The following sections cover the types in the table's order:

- The first six sections each cover one class: [`TableKey`](#tablekey), [`TableLocationKey`](#tablelocationkey), [`AbstractColumnLocation`](#abstractcolumnlocation), [`AbstractTableLocation`](#abstracttablelocation), [`AbstractTableLocationProvider`](#abstracttablelocationprovider), and [`AbstractTableDataService`](#abstracttabledataservice). Each section describes the interface's role and the methods your class must supply.
- Each code block uses classes from the blocks above it, so run the blocks in order.
- The last section, [Example](#example), constructs a static and a refreshing [`PartitionAwareSourceTable`](/core/javadoc/io/deephaven/engine/table/impl/PartitionAwareSourceTable.html) from those classes, using an in-memory data store, and then adds data to show the refreshing table update.

## `TableKey`

A [`TableKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableKey.html) is a unique identifier for a logical table. Implement [`ImmutableTableKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/ImmutableTableKey.html), a sub-interface of `TableKey`. It provides a default `makeImmutable` that returns `this`. You must provide:

- `append(LogOutput)`: Describes the key in log output by writing to the given [`LogOutput`](/core/javadoc/io/deephaven/base/log/LogOutput.html).
- `hashCode` and `equals`: Hash and compare keys. The two must be consistent with each other.

The example also overrides `getImplementationName`, which returns a human-readable name for logging. Overriding it is optional, because the default returns the class's simple name.

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

## `TableLocationKey`

A [`TableLocationKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationKey.html) identifies a specific partition within a table. Extend [`PartitionedTableLocationKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/PartitionedTableLocationKey.html), which handles partition-map storage, sorting, `hashCode`, and `equals`. You provide only `append(LogOutput)`. The example also overrides the optional `getImplementationName`:

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

For an unpartitioned table with a single location, use the built-in singleton returned by [`StandaloneTableLocationKey.getInstance`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/StandaloneTableLocationKey.html).

## `AbstractColumnLocation`

A [`ColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/ColumnLocation.html) provides the actual column data for one partition. The `makeColumnLocation` method of your [`AbstractTableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocation.html) subclass creates it on demand. The column location's `makeColumnRegion*` methods return typed [`ColumnRegion`](/core/javadoc/io/deephaven/engine/table/impl/sources/regioned/ColumnRegion.html) objects that the engine reads data from.

Extend [`AbstractColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractColumnLocation.html) and implement all eight `makeColumnRegion*` methods and `exists`. The `exists` method reports whether the column has data at this location. For the types in your schema, return an [`AppendOnlyFixedSizePageRegion*`](/core/javadoc/io/deephaven/generic/region/package-summary.html) backed by an [`AppendOnlyRegionAccessor`](/core/javadoc/io/deephaven/generic/region/AppendOnlyRegionAccessor.html) that reads from your data store. For the other types, throw `UnsupportedOperationException`. The engine requests each region by the column's storage type: an `Instant` column calls `makeColumnRegionLong` and reads epoch nanoseconds, and a `Boolean` column calls `makeColumnRegionByte`.

Each region constructor takes three arguments:

- A mask that converts the engine's row keys into row positions within the region. Pass the `ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK` constant of [`RegionedColumnSource`](/core/javadoc/io/deephaven/engine/table/impl/sources/regioned/RegionedColumnSource.html).
- The page size, which is the number of rows the region reads at a time.
- The accessor.

[`AppendOnlyRegionAccessor`](/core/javadoc/io/deephaven/generic/region/AppendOnlyRegionAccessor.html) requires two methods:

- `readChunkPage(firstRowPosition, minimumSize, destination)`: Reads rows starting at `firstRowPosition` into `destination`, beginning at chunk position 0. It must read at least `minimumSize` rows and no more than the destination's capacity, and it must set the chunk's size to the number of rows it read. The example resets the size to 0 and then calls `add` once for each row.
- `size`: Returns the total number of rows at this location.

The following example stores column data in typed arrays. The table location passes in a map that keys each array by column name. Each accessor looks up its array in that map on every read, and its `size` method returns the location's current row count, so the regions see rows that the location appends later:

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
    // The table location replaces an array with a longer copy when rows are appended
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
        def location = getTableLocation()
        def name = getName()
        new AppendOnlyFixedSizePageRegionDouble<>(REGION_MASK, PAGE_SIZE,
            new AppendOnlyRegionAccessor<Values>() {
                @Override
                void readChunkPage(long firstRowPosition, int minimumSize, WritableChunk<Values> destination) {
                    double[] data = (double[]) columnArrays[name]
                    destination.setSize(0)
                    for (int i = 0; i < minimumSize; i++) {
                        destination.asWritableDoubleChunk().add(data[(int) (firstRowPosition + i)])
                    }
                }

                @Override
                long size() { location.getSize() }
            })
    }

    @Override
    ColumnRegionLong<Values> makeColumnRegionLong(ColumnDefinition<?> columnDefinition) {
        def location = getTableLocation()
        def name = getName()
        new AppendOnlyFixedSizePageRegionLong<>(REGION_MASK, PAGE_SIZE,
            new AppendOnlyRegionAccessor<Values>() {
                @Override
                void readChunkPage(long firstRowPosition, int minimumSize, WritableChunk<Values> destination) {
                    long[] data = (long[]) columnArrays[name]
                    destination.setSize(0)
                    for (int i = 0; i < minimumSize; i++) {
                        destination.asWritableLongChunk().add(data[(int) (firstRowPosition + i)])
                    }
                }

                @Override
                long size() { location.getSize() }
            })
    }

    @Override
    ColumnRegionObject makeColumnRegionObject(ColumnDefinition columnDefinition) {
        def location = getTableLocation()
        def name = getName()
        new AppendOnlyFixedSizePageRegionObject<>(REGION_MASK, PAGE_SIZE,
            new AppendOnlyRegionAccessor<Values>() {
                @Override
                void readChunkPage(long firstRowPosition, int minimumSize, WritableChunk<Values> destination) {
                    Object[] data = (Object[]) columnArrays[name]
                    destination.setSize(0)
                    for (int i = 0; i < minimumSize; i++) {
                        destination.asWritableObjectChunk().add(data[(int) (firstRowPosition + i)])
                    }
                }

                @Override
                long size() { location.getSize() }
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

## `AbstractTableLocation`

A [`TableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html) represents one partition of a table. It reports the partition's row count to the engine and creates [`ColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/ColumnLocation.html) objects on demand.

The engine subscribes to a table location when a query needs updates from it. The first subscription activates the location's underlying data source, and the data source deactivates when the last subscriber detaches. Each subscription carries a token, and a location uses it to ignore callbacks that arrive from an earlier subscription.

Extend [`AbstractTableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocation.html) and implement:

- `makeColumnLocation(name)`: Returns a [`ColumnLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/ColumnLocation.html) for the named column.
- `refresh`: Re-checks the row count. Call `handleUpdate(RowSet, timestamp)` to report the current size. The [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) argument holds the location's rows.
- `activateUnderlyingDataSource`: Runs when the first subscriber attaches. Initialize the subscription state and call `activationSuccessful`.
- `deactivateUnderlyingDataSource`: Runs when all subscribers detach. Clear the subscription state.
- `matchSubscriptionToken(token)`: Returns `true` if the token matches the current subscription.
- `getSortedColumns`, `getDataIndexColumns`, `hasDataIndex`, `loadDataIndex`: Report sort order and [data index](../data-indexes.md) support. If the location supports neither, return empty lists from the first two, `false` from `hasDataIndex`, and `null` from `loadDataIndex`, as the example does.
- `tryRetainReference`, `dropReference`, `getWeakReference`: Override these [`LivenessReferent`](/core/javadoc/io/deephaven/engine/liveness/LivenessReferent.html) methods and delegate to `asLivenessReferent`, as the example does. `AbstractTableLocation` gets Java default implementations of them from [`DelegatingLivenessReferent`](/core/javadoc/io/deephaven/engine/liveness/DelegatingLivenessReferent.html), but a Groovy subclass doesn't pick up those defaults, so the class fails to compile without the overrides.

You need `activateUnderlyingDataSource`, `deactivateUnderlyingDataSource`, and `matchSubscriptionToken` only when the constructor's `supportsSubscriptions` argument is `true`, as it is in this example.

The example also adds an `appendRows` method, which a refreshing table needs. It replaces each column's array with a longer copy, and then calls `handleUpdate` to report the new row count to the location's subscribers. A static table never sees the new rows, because it reads the row count only once.

```groovy test-set=1 order=null
import io.deephaven.api.SortColumn
import io.deephaven.engine.rowset.RowSetFactory
import io.deephaven.engine.table.BasicDataIndex
import io.deephaven.engine.table.impl.locations.TableKey
import io.deephaven.engine.table.impl.locations.TableLocationKey
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation
import java.lang.reflect.Array
import java.util.concurrent.ConcurrentHashMap

class StockTableLocation extends AbstractTableLocation {
    private volatile Object subscriptionToken
    // Maps column name -> typed array; shared with this location's column locations
    private final Map<String, Object> columnArrays = new ConcurrentHashMap<>()
    private volatile long rowCount

    StockTableLocation(TableKey tableKey, TableLocationKey locationKey,
                       Map<String, Object> columnArrays) {
        super(tableKey, locationKey, true /* supportsSubscriptions */)
        this.columnArrays.putAll(columnArrays)
        this.rowCount = Array.getLength(columnArrays.values().first())
    }

    // Appends rows to every column, then reports the new row count to subscribers
    synchronized void appendRows(Map<String, Object> newRows) {
        newRows.each { name, values -> columnArrays[name] = concat(columnArrays[name], values) }
        rowCount += Array.getLength(newRows.values().first())
        handleUpdate(RowSetFactory.flat(rowCount), System.currentTimeMillis())
    }

    // Returns a new typed array that holds the elements of first followed by the elements of second
    static Object concat(Object first, Object second) {
        int firstLength = Array.getLength(first)
        int secondLength = Array.getLength(second)
        def result = Array.newInstance(first.getClass().getComponentType(), firstLength + secondLength)
        System.arraycopy(first, 0, result, 0, firstLength)
        System.arraycopy(second, 0, result, firstLength, secondLength)
        result
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

    // TableLocation inherits these LivenessReferent methods as abstract, and DelegatingLivenessReferent supplies
    // default implementations. Groovy doesn't use those defaults, so this class overrides them.
    @Override boolean tryRetainReference() { asLivenessReferent().tryRetainReference() }
    @Override void dropReference() { asLivenessReferent().dropReference() }
    @Override java.lang.ref.WeakReference getWeakReference() {
        def delegateRef = asLivenessReferent().getWeakReference()
        // Keep the delegate's weak reference reachable, as the default implementation does
        new java.lang.ref.WeakReference<StockTableLocation>(this) {
            private final Object heldDelegate = delegateRef
        }
    }

    @Override List<SortColumn> getSortedColumns() { [] }
    @Override List<String[]> getDataIndexColumns() { [] }
    @Override boolean hasDataIndex(String... columns) { false }
    @Override BasicDataIndex loadDataIndex(String... columns) { null }
    @Override String getImplementationName() { "StockTableLocation" }
}
```

## `AbstractTableLocationProvider`

A [`TableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationProvider.html) discovers and manages the set of [`TableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html) objects for a given [`TableKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableKey.html). Extend [`AbstractTableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocationProvider.html) and implement:

- `makeTableLocation(locationKey)`: Returns the [`TableLocation`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html) for the given [`TableLocationKey`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationKey.html).
- `refresh`: Re-enumerates the locations. Call `handleTableLocationKeyAdded` for each known key.
- `activateUnderlyingDataSource`: Subscribes to the data source and registers the known locations. Call `activationSuccessful` when it finishes.
- `deactivateUnderlyingDataSource`: Unsubscribes from the data source.
- `matchSubscriptionToken(token)`: Returns `true` if the token matches the current subscription.

You need `activateUnderlyingDataSource`, `deactivateUnderlyingDataSource`, and `matchSubscriptionToken` only when the constructor's `supportsSubscriptions` argument is `true`, as it is in this example. They follow the same activation and token rules as the corresponding methods of a [table location](#abstracttablelocation).

The constructor's other two arguments are [`TableUpdateMode`](/core/javadoc/io/deephaven/engine/table/impl/TableUpdateMode.html) values that declare the updates the provider allows. The first covers the set of locations, and the second covers the rows in each location. This example passes `APPEND_ONLY` for both, because it adds locations and appends rows to existing locations, but never removes either. Pass `STATIC` for both if your locations and their rows never change. Pass `ADD_REMOVE` as the first argument only if the provider also removes locations. Rows can't be removed from a location.

The example also adds an `addRows` method. It appends rows to an existing location, or creates a new location for them and calls `handleTableLocationKeyAdded` to report the new location to the provider's subscribers.

```groovy test-set=1 order=null
import io.deephaven.engine.table.impl.TableUpdateMode
import io.deephaven.engine.table.impl.locations.TableKey
import io.deephaven.engine.table.impl.locations.TableLocation
import io.deephaven.engine.table.impl.locations.TableLocationKey
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider
import java.util.concurrent.ConcurrentHashMap

class StockTableLocationProvider extends AbstractTableLocationProvider {
    private volatile Object subscriptionToken
    private final Map<TableLocationKey, StockTableLocation> locations = new ConcurrentHashMap<>()

    StockTableLocationProvider(TableKey tableKey,
                                Map<TableLocationKey, StockTableLocation> locations) {
        super(tableKey, true /* supportsSubscriptions */, TableUpdateMode.APPEND_ONLY, TableUpdateMode.APPEND_ONLY)
        this.locations.putAll(locations)
    }

    // Appends rows to an existing location, or adds a new location that holds them
    void addRows(TableLocationKey locationKey, Map<String, Object> rows) {
        def location = locations[locationKey]
        if (location != null) {
            location.appendRows(rows)
            return
        }
        locations[locationKey] = new StockTableLocation(getKey(), locationKey, rows)
        handleTableLocationKeyAdded(locationKey)
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

## `AbstractTableDataService`

[`AbstractTableDataService`](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableDataService.html) is the service entry point. It caches [`TableLocationProvider`](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationProvider.html) instances, so you implement only `makeTableLocationProvider(TableKey)`:

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

## Example

This example combines the `StockPricesKey`, `DateLocationKey`, `StockColumnLocation`, `StockTableLocation`, `StockTableLocationProvider`, and `StockDataService` classes from the sections above. It builds two [`PartitionAwareSourceTable`](/core/javadoc/io/deephaven/engine/table/impl/PartitionAwareSourceTable.html) objects from the same provider, backed by an in-memory stock prices store with two date partitions. The last constructor argument, an [`UpdateSourceRegistrar`](/core/javadoc/io/deephaven/engine/updategraph/UpdateSourceRegistrar.html), chooses a static or refreshing table:

- `stockPrices` is a static table, because its `UpdateSourceRegistrar` is `null`. It holds the rows the store had when the engine first read the table. The engine calls `refresh` on the provider and on each location once to discover them.
- `stockPricesLive` is a refreshing table. Its `UpdateSourceRegistrar` is the current [`UpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html), which the code gets from the current [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) with `ExecutionContext.getContext().getUpdateGraph()`. The engine subscribes to the provider and to each location, so the table updates as data arrives.

```groovy test-set=1 order=stockPrices,stockPricesLive reset
import io.deephaven.engine.context.ExecutionContext
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
provider = service.getTableLocationProvider(tableKey)

// A null updateSourceRegistrar makes a static table, which calls refresh once
stockPrices = new PartitionAwareSourceTable(
    tableDefinition,
    "StockPrices",
    RegionedTableComponentFactoryImpl.INSTANCE,
    provider,
    null
)

// The update graph as the updateSourceRegistrar makes a refreshing table, which subscribes
stockPricesLive = new PartitionAwareSourceTable(
    tableDefinition,
    "StockPricesLive",
    RegionedTableComponentFactoryImpl.INSTANCE,
    provider,
    ExecutionContext.getContext().getUpdateGraph()
)
```

While the tables are open in the IDE, call the provider's `addRows` method to add more data. The refreshing table `stockPricesLive` shows the new rows on the engine's next update cycle, and the static table `stockPrices` doesn't change.

```groovy test-set=1 order=null reset
// This adds a new partition to the already opened table
provider.addRows(new DateLocationKey("2024-01-03"), [
    "Symbol": ["AAPL", "GOOGL", "MSFT"] as String[],
    "Price":  [151.10d, 2795.25d, 384.00d] as double[],
    "Volume": [900_000L, 550_000L, 700_000L] as long[]
])

// This appends a row to an existing partition of the already opened table
provider.addRows(new DateLocationKey("2024-01-01"), [
    "Symbol": ["NVDA"] as String[],
    "Price":  [495.22d] as double[],
    "Volume": [400_000L] as long[]
])
```

Each location occupies its own range of row keys, so the `NVDA` row appended to the `2024-01-01` partition appears in `stockPricesLive` before the rows of the `2024-01-02` partition, not at the end of the table. Because rows don't always arrive at the end, the table is [add-only](../../conceptual/table-types.md#specialization-2-add-only) but not [append-only](../../conceptual/table-types.md#specialization-1-append-only).

To make your own provider and locations support a refreshing table, they need the same three things that the example has:

- The provider calls `handleTableLocationKeyAdded` when your data store gains a location, and each location calls `handleUpdate` when its row count grows.
- The column regions read from a data store that can grow. Each accessor's `size` method returns the location's current row count, and `readChunkPage` reads from the current data.
- The provider's constructor passes [update modes](#abstracttablelocationprovider) that allow those changes, such as `APPEND_ONLY` for both arguments.

## Related documentation

- [`AbstractColumnLocation` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractColumnLocation.html)
- [`AbstractTableDataService` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableDataService.html)
- [`AbstractTableLocation` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocation.html)
- [`AbstractTableLocationProvider` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/AbstractTableLocationProvider.html)
- [`AppendOnlyRegionAccessor` Javadoc](/core/javadoc/io/deephaven/generic/region/AppendOnlyRegionAccessor.html)
- [`ColumnLocation` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/ColumnLocation.html)
- [`ColumnRegion` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/sources/regioned/ColumnRegion.html)
- [Custom data sources](../../reference/community-questions/custom-data-sources.md)
- [Data indexes](../data-indexes.md)
- [`ImmutableTableKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/ImmutableTableKey.html)
- [`PartitionAwareSourceTable` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/PartitionAwareSourceTable.html)
- [Partitioned tables](../partitioned-tables.md)
- [`PartitionedTableLocationKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/PartitionedTableLocationKey.html)
- [`StandaloneTableLocationKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/impl/StandaloneTableLocationKey.html)
- [Table types](../../conceptual/table-types.md)
- [`TableDataService` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableDataService.html)
- [`TableKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableKey.html)
- [`TableLocation` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocation.html)
- [`TableLocationKey` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationKey.html)
- [`TableLocationProvider` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/locations/TableLocationProvider.html)
