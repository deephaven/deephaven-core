//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.dataadapter.consumers.*;
import io.deephaven.dataadapter.locking.GetDataLockType;
import io.deephaven.dataadapter.locking.QueryDataRetrievalOperation;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import io.deephaven.dataadapter.rec.updaters.*;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.dataindex.TableBackedDataIndex;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.function.ThrowingBiConsumer;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Adapter for using {@link io.deephaven.engine.table.Table Tables} like hashmaps.
 * <p>
 * Instantiate through {@link #makeRecordAdapterSimpleKey}/{@link #makeRecordAdapterCompositeKey}.
 *
 * @param <K> Key type. If there is one key column, {@code K} must be that column's type. If there are multiple key
 *        columns, {@code K} must be {@code List<?>}.
 * @param <T> Record type. Each retrieved row is represented by one instance of {@code T}.
 */
public class KeyedRecordAdapter<K, T> {

    /**
     * Whether trace logging should be enabled. Used when testing or debugging.
     */
    private final boolean TRACE_LOGGING = Boolean.parseBoolean(System.getProperty("KeyedRecordAdapter.trace", "false"));

    /**
     * Consumer that obtains lock before calling code
     */
    @NotNull
    protected final ThrowingBiConsumer<QueryDataRetrievalOperation, String, RuntimeException> DO_LOCKED_FUNCTION;

    /**
     * Function to convert a list of user-friendly keys to a list of lookup keys that can be used with a
     * {@link DataIndex} or {@link AggregationRowLookup} (which operate on the base storage types for reinterpreted
     * types -- see {@link ReinterpretUtils} or {@link #getKeyReinterpreter}). The "user-friendly keys" must be either a
     * {@code List<K>} (if there is one {@link #keyColumns key column}) or a {@code List<List<?>>} (if there are
     * multiple key columns).
     */
    @NotNull
    protected final Function<List<?>, List<Object>> dataKeysListToLookupKeys;

    /**
     * The keys by which records are identified.
     */
    @NotNull
    private final String[] keyColumns;

    /**
     * Utility for creating the records and populating them with data. Data is extracted from the table with the
     * {@link MultiRowRecordAdapter#getTableDataArrayRetriever()}.
     */
    @NotNull
    protected final MultiRowRecordAdapter<T> multiRowRecordAdapter;

    /**
     * Consumer to update a record with key data (e.g. populating the record fields that correspond to the
     * {@link #keyColumns}). This is separate from singleRowRecordAdapter/multiRowRecordAdapter because the keys do not
     * need to be retrieved from the table (since any key used to retrieve data is already in memory).
     */
    @Nullable
    private final BiConsumer<T, K> recordKeyDataUpdater;

    /**
     * Whether this KeyedRecordAdapter has just one key column (as opposed to multiple key columns).
     */
    protected final boolean isSingleKeyCol;

    /**
     * Function called (under the lock or in a snapshot) to look up the row keys corresponding to each requested data
     * key.
     */
    private final RowSetRetriever targetRowSetRetriever;

    /**
     * Create a KeyedRecordAdapter that translates rows of {@code sourceTable} into instances of type {@code T} using
     * the provided {@code recordAdapterDescriptor}, keyed by the given {@code keyColumns}.
     *
     * @param sourceTable The table whose data will be used to create records.
     * @param rowRecordAdapterDescriptor The descriptor used to build the record adapter to read data from
     *        {@code sourceTable} into records
     * @param keyColumns The key columns to use when retrieving data
     */
    protected KeyedRecordAdapter(@NotNull Table sourceTable,
            @NotNull RecordAdapterDescriptor<T> rowRecordAdapterDescriptor, @NotNull String... keyColumns) {
        this(sourceTable, null, null, rowRecordAdapterDescriptor, keyColumns);
    }

    /**
     * Internal constructor for creating a KeyedRecordAdapter, from either a regular table ({@code theTable}) or a
     * partitioned table ({@code thePartitionedTable}). The following configurations are supported:
     * <ul>
     * <li>Regular table with a data index.</li>
     * <li>Regular table with an AggregationRowLookup (e.g. an aggregation result).</li>
     * <li>Regular table <i>without</i>, an AggregationRowLookup, in which case we automatically call .lastBy() on
     * it.</li>
     * <li>Partitioned table with an AggregationRowLookup (i.e., created with {@link Table#partitionBy})</li>
     * </ul>
     *
     * @param theTable The table whose data will be used to create records. Exactly one of {@code theTable} and
     *        {@code thePartitionedTable} must be non-null.
     * @param thePartitionedTable The partitioned table whose data will be used to create records. Exactly one of
     *        {@code theTable} and {@code thePartitionedTable} must be non-null.
     * @param dataIndex The data index to use for looking up rows corresponding to data keys.
     * @param rowRecordAdapterDescriptor The descriptor used to build the record adapter for reading data from the
     *        table.
     * @param keyColumns The key columns to use when retrieving data. These values in these columns are the "data keys"
     *        (to differentiate from row keys).
     */
    private KeyedRecordAdapter(
            @Nullable final Table theTable,
            @Nullable final PartitionedTable thePartitionedTable,
            @Nullable final DataIndex dataIndex,
            @NotNull final RecordAdapterDescriptor<T> rowRecordAdapterDescriptor,
            @NotNull final String... keyColumns) {
        if (theTable != null && thePartitionedTable != null) {
            throw new IllegalArgumentException("Only one of theTable and thePartitionedTable may be non-null!");
        }

        if (keyColumns.length == 0) {
            throw new IllegalArgumentException("At least one key column must be provided!");
        }

        this.keyColumns = keyColumns;

        final Table sourceTable;

        if (dataIndex != null) {
            Assert.eqTrue(
                    dataIndex.keyColumnNames().equals(Arrays.asList(keyColumns)),
                    "dataIndex.keyColumnNames().equals(Arrays.asList(keyColumns))");
            sourceTable = theTable;
            // TODO: assert that the dataIndex is actually built on 'theTable'?
        } else if (thePartitionedTable != null) {
            // TODO: for partitioned tables we need to worry about constituentChangesPermitted, which is independent of
            // refreshingness of the constituent
            sourceTable = thePartitionedTable.table();

            // Partitioned tables must have an AggregationRowLookup.
            Assert.eqTrue(sourceTable.hasAttribute(Table.AGGREGATION_ROW_LOOKUP_ATTRIBUTE),
                    "sourceTable.hasAttribute(Table.AGGREGATION_ROW_LOOKUP_ATTRIBUTE)");

            // make sure we have the expected keys
            sourceTable.getDefinition().checkHasColumns(List.of(keyColumns));
        } else {
            Objects.requireNonNull(theTable, "theTable");
            if (theTable.hasAttribute(Table.AGGREGATION_ROW_LOOKUP_ATTRIBUTE)) {
                sourceTable = theTable;
            } else {
                sourceTable = theTable.lastBy(keyColumns);
            }
        }

        isSingleKeyCol = keyColumns.length == 1;

        final ColumnSource<?>[] keyColumnSources = Arrays.stream(keyColumns)
                .map(sourceTable::getColumnSource)
                .toArray(ColumnSource[]::new);

        final ColumnSource<?>[] keyColumnSourcesReinterpreted =
                Arrays.stream(keyColumnSources)
                        .map(ReinterpretUtils::maybeConvertToPrimitive)
                        .toArray(ColumnSource[]::new);

        // There must be either a data index or an AggregationRowLookup.
        if (dataIndex != null) {
            final DataIndex.RowKeyLookup rowKeyLookup = dataIndex.rowKeyLookup(keyColumnSourcesReinterpreted);
            final ColumnSource<RowSet> dataIndexRowSetColSource =
                    dataIndex.table().getColumnSource(dataIndex.rowSetColumnName());

            final LongSupplier approxRowsPerKey = () -> sourceTable.size() / dataIndex.table().size();

            targetRowSetRetriever = (keysList, usePrev) -> getRowSetForKeysWithDataIndex(
                    approxRowsPerKey,
                    rowKeyLookup,
                    dataIndexRowSetColSource,
                    usePrev,
                    keysList);
        } else {
            final AggregationRowLookup rowLookup = AggregationProcessor.getRowLookup(sourceTable);
            targetRowSetRetriever = (keysList, usePrev) -> getRowSetForKeysFromAggregatedTable(
                    sourceTable,
                    rowLookup,
                    usePrev,
                    keysList);
        }

        final NotificationStepSource[] notificationSources;
        if (dataIndex != null) {
            notificationSources = new NotificationStepSource[] {
                    (BaseTable<?>) sourceTable,
                    (BaseTable<?>) dataIndex.table()
            };
        } else {
            // Note: for partitioned table, only need to depend on the partitioned table itself, since the partitioned
            // table depends on its constituents
            notificationSources = new NotificationStepSource[] {(BaseTable<?>) sourceTable};
        }

        DO_LOCKED_FUNCTION = GetDataLockType.getDoLockedConsumer(
                sourceTable.getUpdateGraph(),
                GetDataLockType.SNAPSHOT,
                notificationSources);

        // Create a record adapter that excludes the key columns, if they are present, (since all available keys will
        // already be in memory)
        final RecordAdapterDescriptorBuilder<T> recordAdapterDescriptorBuilderNoKeys =
                RecordAdapterDescriptorBuilder.create(rowRecordAdapterDescriptor);

        // Remove the key columns from the new record adapter descriptor and put them in a map.
        // (Reading key columns out of table would be redundant, and in the case of PartitionedTables they're not
        // necessarily even present in the constituents.)
        final Map<String, RecordUpdater<T, ?>> keyColRecordUpdaters = new LinkedHashMap<>();
        for (String keyColName : keyColumns) {
            final RecordUpdater<T, ?> keyColRecordUpdater =
                    recordAdapterDescriptorBuilderNoKeys.removeColumn(keyColName);
            if (keyColRecordUpdater != null) {
                keyColRecordUpdaters.put(keyColName, keyColRecordUpdater);
            }
        }

        // Get a record adapter descriptor that deals with the raw data (without the keys -- since they can be populated
        // afterward)
        final RecordAdapterDescriptor<T> recordAdapterDescriptorNoKeys = recordAdapterDescriptorBuilderNoKeys.build();

        if (thePartitionedTable != null) {
            multiRowRecordAdapter = recordAdapterDescriptorNoKeys.createMultiRowRecordAdapter(thePartitionedTable);
        } else {
            multiRowRecordAdapter = recordAdapterDescriptorNoKeys.createMultiRowRecordAdapter(sourceTable);
        }

        final Class<?>[] keyColumnTypes =
                Arrays.stream(keyColumnSources).map(ColumnSource::getType).toArray(Class[]::new);

        // Function for converting a data key to the corresponding reinterpreted types (to be used as a lookup key).
        final Function<Object, Object> dataKeyReinterpreter = getKeyReinterpreter(keyColumnTypes);

        // Set the dataKeyToMapKey, dataKeysListToMapKeys, and updateRecordWithKeyData lambdas depending on
        // whether the key is a simple key from a single column or a composite key from multiple columns:
        if (isSingleKeyCol) {
            /*
             * the contents of the dataKeys list are the map keys themselves. dataKeys is the list of keys that we want
             * to look up in the ToMapListener.
             */

            // a list of keys passed to getRecords() will be just that -- a List<K>
            dataKeysListToLookupKeys =
                    dataKeys -> dataKeys.stream().map(dataKeyReinterpreter).collect(Collectors.toList());

            // noinspection unchecked
            final RecordUpdater<T, K> keyColRecordUpdater =
                    (RecordUpdater<T, K>) keyColRecordUpdaters.get(keyColumns[0]);
            if (keyColRecordUpdater != null) {
                final Class<?> keyColumnType = keyColumnSources[0].getType();
                final Class<?> expectedType = keyColRecordUpdater.getSourceType();
                if (!expectedType.isAssignableFrom(keyColumnType)) {
                    throw new IllegalArgumentException(
                            "Key column 0: Record updater expects type " + expectedType + ", instead table has type " +
                                    keyColumnType.getCanonicalName());
                }

                recordKeyDataUpdater = getSingleKeyRecordUpdater(keyColRecordUpdater);
            } else {
                recordKeyDataUpdater = null;
            }
        } else {
            // a list of keys passed to getRecords() will be a List<List<?>>; need extract
            // arrays from the inner lists before calling the dataKeyReinterpreter
            dataKeysListToLookupKeys = dataKeys -> dataKeys
                    .stream()
                    .map(compositeDataKey -> ((List<?>) compositeDataKey).toArray())
                    .map(dataKeyReinterpreter)
                    .collect(Collectors.toList());

            final List<BiConsumer<T, List<?>>> keyDataUpdaters = new ArrayList<>(keyColumns.length);
            for (int i = 0; i < keyColumns.length; i++) {
                String keyColumn = keyColumns[i];
                final RecordUpdater<T, ?> keyColRecordUpdater = keyColRecordUpdaters.get(keyColumn);
                if (keyColRecordUpdater != null) {
                    final Class<?> keyColumnType = keyColumnSources[i].getType();
                    final Class<?> expectedType = keyColRecordUpdater.getSourceType();
                    if (!expectedType.isAssignableFrom(keyColumnType)) {
                        throw new IllegalArgumentException(
                                "Key column " + i + ": Record updater expects type " + expectedType
                                        + ", instead table has type " +
                                        keyColumnType.getCanonicalName());
                    }

                    keyDataUpdaters.add(getCompositeKeyRecordUpdater(i, keyColRecordUpdater));
                }
            }

            if (keyDataUpdaters.isEmpty()) {
                recordKeyDataUpdater = null;
            } else {
                recordKeyDataUpdater =
                        (r, k) -> keyDataUpdaters.forEach(recordKeyUpdater -> recordKeyUpdater.accept(r, (List<?>) k));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <R, K> BiConsumer<R, K> getSingleKeyRecordUpdater(RecordUpdater<R, K> keyColRecordUpdater) {
        BiConsumer<R, ?> result;
        if (keyColRecordUpdater instanceof ByteRecordUpdater) {
            ObjByteConsumer<R> keyColRecordUpdaterCast = (ObjByteConsumer<R>) keyColRecordUpdater;
            result = (R record, Byte v) -> keyColRecordUpdaterCast.accept(record, TypeUtils.unbox(v));
        } else if (keyColRecordUpdater instanceof CharRecordUpdater) {
            ObjCharConsumer<R> keyColRecordUpdaterCast = (ObjCharConsumer<R>) keyColRecordUpdater;
            result = (R record, Character v) -> keyColRecordUpdaterCast.accept(record, TypeUtils.unbox(v));
        } else if (keyColRecordUpdater instanceof ShortRecordUpdater) {
            ObjShortConsumer<R> keyColRecordUpdaterCast = (ObjShortConsumer<R>) keyColRecordUpdater;
            result = (R record, Short v) -> keyColRecordUpdaterCast.accept(record, TypeUtils.unbox(v));
        } else if (keyColRecordUpdater instanceof IntRecordUpdater) {
            ObjIntConsumer<R> keyColRecordUpdaterCast = (ObjIntConsumer<R>) keyColRecordUpdater;
            result = (R record, Integer v) -> keyColRecordUpdaterCast.accept(record, TypeUtils.unbox(v));
        } else if (keyColRecordUpdater instanceof FloatRecordUpdater) {
            ObjFloatConsumer<R> keyColRecordUpdaterCast = (ObjFloatConsumer<R>) keyColRecordUpdater;
            result = (R record, Float v) -> keyColRecordUpdaterCast.accept(record, TypeUtils.unbox(v));
        } else if (keyColRecordUpdater instanceof LongRecordUpdater) {
            ObjLongConsumer<R> keyColRecordUpdaterCast = (ObjLongConsumer<R>) keyColRecordUpdater;
            result = (R record, Long v) -> keyColRecordUpdaterCast.accept(record, TypeUtils.unbox(v));
        } else if (keyColRecordUpdater instanceof DoubleRecordUpdater) {
            ObjDoubleConsumer<R> keyColRecordUpdaterCast = (ObjDoubleConsumer<R>) keyColRecordUpdater;
            result = (R record, Double v) -> keyColRecordUpdaterCast.accept(record, TypeUtils.unbox(v));
        } else if (keyColRecordUpdater instanceof ObjRecordUpdater) {
            result = ((BiConsumer<R, Object>) keyColRecordUpdater);
        } else {
            throw new IllegalStateException("Unexpected updater type: " + keyColRecordUpdater.getClass());
        }

        return (BiConsumer<R, K>) result;
    }

    /**
     * @param keyIndex RowSet of composite key component (corresponding to the {@link #keyColumns})
     * @param keyColRecordUpdater The record updater for the key columns at index {@code keyIndex}
     */
    @SuppressWarnings("unchecked")
    private static <R> BiConsumer<R, List<?>> getCompositeKeyRecordUpdater(int keyIndex,
            RecordUpdater<R, ?> keyColRecordUpdater) {
        if (keyColRecordUpdater instanceof ByteRecordUpdater) {
            ByteRecordUpdater<R> keyColRecordUpdaterCast = (ByteRecordUpdater<R>) keyColRecordUpdater;
            return (R record, List<?> v) -> keyColRecordUpdaterCast.accept(record,
                    TypeUtils.unbox((Byte) v.get(keyIndex)));
        } else if (keyColRecordUpdater instanceof CharRecordUpdater) {
            CharRecordUpdater<R> keyColRecordUpdaterCast = (CharRecordUpdater<R>) keyColRecordUpdater;
            return (R record, List<?> v) -> keyColRecordUpdaterCast.accept(record,
                    TypeUtils.unbox((Character) v.get(keyIndex)));
        } else if (keyColRecordUpdater instanceof ShortRecordUpdater) {
            ShortRecordUpdater<R> keyColRecordUpdaterCast = (ShortRecordUpdater<R>) keyColRecordUpdater;
            return (R record, List<?> v) -> keyColRecordUpdaterCast.accept(record,
                    TypeUtils.unbox((Short) v.get(keyIndex)));
        } else if (keyColRecordUpdater instanceof IntRecordUpdater) {
            IntRecordUpdater<R> keyColRecordUpdaterCast = (IntRecordUpdater<R>) keyColRecordUpdater;
            return (R record, List<?> v) -> keyColRecordUpdaterCast.accept(record,
                    TypeUtils.unbox((Integer) v.get(keyIndex)));
        } else if (keyColRecordUpdater instanceof FloatRecordUpdater) {
            FloatRecordUpdater<R> keyColRecordUpdaterCast = (FloatRecordUpdater<R>) keyColRecordUpdater;
            return (R record, List<?> v) -> keyColRecordUpdaterCast.accept(record,
                    TypeUtils.unbox((Float) v.get(keyIndex)));
        } else if (keyColRecordUpdater instanceof LongRecordUpdater) {
            LongRecordUpdater<R> keyColRecordUpdaterCast = (LongRecordUpdater<R>) keyColRecordUpdater;
            return (R record, List<?> v) -> keyColRecordUpdaterCast.accept(record,
                    TypeUtils.unbox((Long) v.get(keyIndex)));
        } else if (keyColRecordUpdater instanceof DoubleRecordUpdater) {
            DoubleRecordUpdater<R> keyColRecordUpdaterCast = (DoubleRecordUpdater<R>) keyColRecordUpdater;
            return (R record, List<?> v) -> keyColRecordUpdaterCast.accept(record,
                    TypeUtils.unbox((Double) v.get(keyIndex)));
        } else if (keyColRecordUpdater instanceof ObjRecordUpdater) {
            // noinspection unchecked
            return (R record, List<?> v) -> ((ObjRecordUpdater<R, Object>) keyColRecordUpdater).accept(record,
                    v.get(keyIndex));
        } else {
            throw new IllegalStateException("Unexpected updater type: " + keyColRecordUpdater.getClass());
        }
    }

    public static KeyedRecordAdapter<List<?>, Map<String, Object>> makeRecordAdapterCompositeKey(Table sourceTable,
            List<String> valueColumns, String... keyColumns) {
        final ArrayList<String> allCols = new ArrayList<>(keyColumns.length + valueColumns.size());
        allCols.addAll(Arrays.asList(keyColumns));
        allCols.addAll(valueColumns);
        final RecordAdapterDescriptor<Map<String, Object>> genericRecordAdapterDescriptor =
                RecordAdapterDescriptor.createGenericRecordAdapterDescriptor(sourceTable, allCols);
        return new KeyedRecordAdapter<>(sourceTable, genericRecordAdapterDescriptor, keyColumns);
    }

    public static KeyedRecordAdapter<List<?>, Map<String, Object>> makeRecordAdapterCompositeKey(Table sourceTable,
            TableBackedDataIndex dataIndex,
            List<String> valueColumns) {
        final List<String> keyColumnsList = dataIndex.keyColumnNames();
        final String[] keyColumns = keyColumnsList.toArray(new String[0]);
        final ArrayList<String> allCols = new ArrayList<>(keyColumns.length + valueColumns.size());
        allCols.addAll(keyColumnsList);
        allCols.addAll(valueColumns);
        final RecordAdapterDescriptor<Map<String, Object>> genericRecordAdapterDescriptor =
                RecordAdapterDescriptor.createGenericRecordAdapterDescriptor(sourceTable, allCols);
        return new KeyedRecordAdapter<>(sourceTable, null, dataIndex, genericRecordAdapterDescriptor, keyColumns);
    }

    public static KeyedRecordAdapter<List<?>, Map<String, Object>> makeRecordAdapterCompositeKey(
            PartitionedTable sourceTable,
            List<String> valueColumns) {
        final List<String> keyColumnsList = new ArrayList<>(sourceTable.keyColumnNames());

        if (keyColumnsList.isEmpty()) {
            throw new IllegalArgumentException("partitionedTable has no key columns!");
        }
        if (keyColumnsList.size() == 1) {
            throw new IllegalArgumentException(
                    "Attempting to create composite-key KeyedRecordAdapter but partitionedTable has only one key column. Use makeRecordAdapterSimpleKey instead.");
        }

        final String[] keyColumns = keyColumnsList.toArray(new String[0]);
        final ArrayList<String> allCols = new ArrayList<>(keyColumns.length + valueColumns.size());
        allCols.addAll(keyColumnsList);
        allCols.addAll(valueColumns);

        // Note: this requires that the constituents include the key columns -- i.e., the partitioned table is created
        // with .partitionBy(source) or .partitionBy(false, source)
        final RecordAdapterDescriptor<Map<String, Object>> genericRecordAdapterDescriptor =
                RecordAdapterDescriptor.createGenericRecordAdapterDescriptor(sourceTable.constituentDefinition(),
                        allCols);
        return new KeyedRecordAdapter<>(null, sourceTable, null, genericRecordAdapterDescriptor, keyColumns);
    }

    public static <T> KeyedRecordAdapter<List<?>, T> makeRecordAdapterCompositeKey(Table sourceTable,
            RecordAdapterDescriptor<T> recordAdapterDescriptor, String... keyColumns) {
        return new KeyedRecordAdapter<>(sourceTable, recordAdapterDescriptor, keyColumns);
    }

    public static <T> KeyedRecordAdapter<List<?>, T> makeRecordAdapterCompositeKey(Table sourceTable,
            TableBackedDataIndex dataIndex,
            RecordAdapterDescriptor<T> recordAdapterDescriptor) {
        final List<String> keyColumnsList = dataIndex.keyColumnNames();

        if (keyColumnsList.isEmpty()) {
            throw new IllegalArgumentException("dataIndex has no key columns!");
        }
        if (keyColumnsList.size() == 1) {
            throw new IllegalArgumentException(
                    "Attempting to create composite-key KeyedRecordAdapter but dataIndex has only one key column. Use makeRecordAdapterSimpleKey instead.");
        }

        final String[] keyColumns = keyColumnsList.toArray(new String[0]);
        return new KeyedRecordAdapter<>(sourceTable, null, dataIndex, recordAdapterDescriptor, keyColumns);
    }

    public static <T> KeyedRecordAdapter<List<?>, T> makeRecordAdapterCompositeKey(PartitionedTable sourceTable,
            RecordAdapterDescriptor<T> recordAdapterDescriptor) {
        final List<String> keyColumnsList = new ArrayList<>(sourceTable.keyColumnNames());

        if (keyColumnsList.isEmpty()) {
            throw new IllegalArgumentException("sourceTable has no key columns!");
        }
        if (keyColumnsList.size() == 1) {
            throw new IllegalArgumentException(
                    "Attempting to create composite-key KeyedRecordAdapter but sourceTable has only one key column. Use makeRecordAdapterSimpleKey instead.");
        }

        final String[] keyColumns = keyColumnsList.toArray(new String[0]);
        return new KeyedRecordAdapter<>(null, sourceTable, null, recordAdapterDescriptor, keyColumns);
    }

    public static <K, T> KeyedRecordAdapter<K, T> makeRecordAdapterSimpleKey(Table sourceTable,
            RecordAdapterDescriptor<T> recordAdapterDescriptor, String keyColumn, Class<K> keyColType) {
        // throw an exception if keyColumn is not of keyColType:
        sourceTable.getColumnSource(keyColumn, TypeUtils.getUnboxedTypeIfBoxed(keyColType));
        return new KeyedRecordAdapter<>(sourceTable, recordAdapterDescriptor, keyColumn);
    }

    public static <K, T> KeyedRecordAdapter<K, T> makeRecordAdapterSimpleKey(Table sourceTable,
            TableBackedDataIndex dataIndex,
            RecordAdapterDescriptor<T> recordAdapterDescriptor,
            Class<K> keyColType

    ) {
        final List<String> keyColumnsList = dataIndex.keyColumnNames();

        if (keyColumnsList.isEmpty()) {
            throw new IllegalArgumentException("dataIndex has no key columns!");
        }
        if (keyColumnsList.size() > 1) {
            throw new IllegalArgumentException(
                    "Attempting to create simple-key KeyedRecordAdapter but dataIndex has multiple key columns. Use makeRecordAdapterCompositeKey instead.");
        }

        // Ensure the key column type matches between the given keyColType and the dataIndex
        final String colNameInDataIndex = keyColumnsList.get(0);
        final Class<Object> dataIndexKeyColType = dataIndex.table().getColumnSource(colNameInDataIndex).getType();
        if (!keyColType.isAssignableFrom(dataIndexKeyColType)) {
            throw new IllegalArgumentException("Key column type mismatch: expected type " + keyColType.getName()
                    + ", found " + dataIndexKeyColType.getName() + " in dataIndex for column " + colNameInDataIndex);
        }

        final String[] keyColumns = keyColumnsList.toArray(new String[0]);
        return new KeyedRecordAdapter<>(sourceTable, null, dataIndex, recordAdapterDescriptor, keyColumns);
    }

    public static <K, T> KeyedRecordAdapter<K, T> makeRecordAdapterSimpleKey(PartitionedTable sourceTable,
            RecordAdapterDescriptor<T> recordAdapterDescriptor,
            Class<K> keyColType

    ) {
        final List<String> keyColumnsList = new ArrayList<>(sourceTable.keyColumnNames());

        if (keyColumnsList.isEmpty()) {
            throw new IllegalArgumentException("dataIndex has no key columns!");
        }
        if (keyColumnsList.size() > 1) {
            throw new IllegalArgumentException(
                    "Attempting to create simple-key KeyedRecordAdapter but dataIndex has multiple key columns. Use makeRecordAdapterCompositeKey instead.");
        }

        // Ensure the key column type matches between the given keyColType and the dataIndex
        final String colNameInDataIndex = keyColumnsList.get(0);
        final Class<Object> dataIndexKeyColType = sourceTable.table().getColumnSource(colNameInDataIndex).getType();
        if (!keyColType.isAssignableFrom(dataIndexKeyColType)) {
            throw new IllegalArgumentException(
                    "Key column type mismatch: expected type " + keyColType.getName() + ", found "
                            + dataIndexKeyColType.getName() + " in partitioned table for column " + colNameInDataIndex);
        }

        final String[] keyColumns = keyColumnsList.toArray(new String[0]);
        return new KeyedRecordAdapter<>(null, sourceTable, null, recordAdapterDescriptor, keyColumns);
    }

    /**
     * Gets a record for a single key.
     *
     * @param dataKey An array of the key components.
     */
    public T getRecordCompositeKey(final Object... dataKey) {
        Require.neqNull(dataKey, "dataKey");

        // make sure that enough keys are specified
        final int nKeyComponents = dataKey.length;
        final int nCols = keyColumns.length;
        if (nKeyComponents != nCols) {
            throw new IllegalArgumentException("dataKey has " + nKeyComponents + " components; expected " + nCols);
        }

        if (nKeyComponents == 1) {
            // noinspection unchecked
            return getRecord((K) dataKey[0]);
        } else {
            // noinspection unchecked
            return getRecord((K) Arrays.asList(dataKey));
        }
    }

    /**
     * Retrieve a record of type {@code T} corresponding to the data in the table for the given {@code dataKey}. If the
     * table contains multiple records for the given key, the last record will be returned.
     *
     * @param dataKey The key for which to retrieve data
     * @return A record containing data for the key, if the key is present in the table. Otherwise, {@code null}.
     */
    public T getRecord(K dataKey) {
        // noinspection unchecked
        final Map<K, T> records = getRecords(dataKey);
        return records.get(dataKey);
    }

    @SuppressWarnings("unchecked")
    public Map<K, T> getRecords(final K... dataKeys) {
        return getRecords(Arrays.asList(dataKeys));
    }

    /**
     * Gets a record for a single key.
     *
     * @param dataKey An array of the key components.
     */
    public List<T> getRecordListCompositeKey(final Object... dataKey) {
        Require.neqNull(dataKey, "dataKey");

        // make sure that enough keys are specified
        final int nKeyComponents = dataKey.length;
        final int nCols = keyColumns.length;
        if (nKeyComponents != nCols) {
            throw new IllegalArgumentException("dataKey has " + nKeyComponents + " components; expected " + nCols);
        }

        if (nKeyComponents == 1) {
            // noinspection unchecked
            return getRecordList((K) dataKey[0]);
        } else {
            // noinspection unchecked
            return getRecordList((K) Arrays.asList(dataKey));
        }
    }

    /**
     * Retrieve multiple records of type {@code T} corresponding to the data in the table for the given {@code dataKey}.
     * If the table contains multiple records for the given key, the last record will be returned.
     *
     * @param dataKey The key for which to retrieve data
     * @return A List of record containing data for the key, if the key is present in the table. Otherwise,
     *         {@code null}.
     */
    public List<T> getRecordList(K dataKey) {
        // noinspection unchecked
        final Map<K, List<T>> recordsLists = getRecordsLists(dataKey);
        return recordsLists.get(dataKey);
    }

    @SuppressWarnings("unchecked")
    public Map<K, List<T>> getRecordsLists(final K... dataKeys) {
        return getRecordsLists(Arrays.asList(dataKeys));
    }

    /**
     * Retrieve records of type {@code T} corresponding to the data in the table for the given {@code dataKeys}.
     */
    public Map<K, T> getRecords(final List<K> dataKeys) {
        final Result<T> result = getResult(dataKeys);

        // Finish handling the retrieved records.
        // Populate the fields pertaining to the key, and stick the records in a map.
        final TLongIntMap dbIdxKeyToDataKeyPositionalIndex = result.dbRowKeyToDataKeyPositionalIndex;
        final Map<K, T> resultsMap = new HashMap<>(dataKeys.size());
        for (int idx = 0; idx < result.nRetrievedRecords; idx++) {
            // Find the data key corresponding to the DB index key from
            // which this record was retrieved. Map the data key to the record.
            final long rowKeyForRecord = result.recordDataRowKeys.get(idx);
            final int dataKeyIdxForRecord = dbIdxKeyToDataKeyPositionalIndex.get(rowKeyForRecord);
            final K dataKeyForRecord = dataKeys.get(dataKeyIdxForRecord);

            // If the key columns are included in the record, then update them based
            // on the data key (without reading from DB)
            updateRecordWithKeyData(result.recordsArr[idx], dataKeyForRecord);

            resultsMap.put(dataKeyForRecord, result.recordsArr[idx]);
        }

        return resultsMap;
    }

    /**
     * Retrieve records of type {@code T} corresponding to the data in the table for the given {@code dataKeys}.
     */
    public Map<K, List<T>> getRecordsLists(final List<K> dataKeys) {
        final Result<T> result = getResult(dataKeys);

        // Finish handling the retrieved records.
        // Populate the fields pertaining to the key, and stick the records in a map.
        final TLongIntMap dbIdxKeyToDataKeyPositionalIndex = result.dbRowKeyToDataKeyPositionalIndex;
        final Map<K, List<T>> resultsMap = new HashMap<>(dataKeys.size());
        for (int idx = 0; idx < result.nRetrievedRecords; idx++) {
            // Find the data key corresponding to the DB index key from
            // which this record was retrieved. Map the data key to the record.
            final long rowKeyForRecord = result.recordDataRowKeys.get(idx);
            final int dataKeyIdxForRecord = dbIdxKeyToDataKeyPositionalIndex.get(rowKeyForRecord);
            final K dataKeyForRecord = dataKeys.get(dataKeyIdxForRecord);

            // If the key columns are included in the record, then update them based
            // on the data key (without reading from DB)
            updateRecordWithKeyData(result.recordsArr[idx], dataKeyForRecord);

            resultsMap.computeIfAbsent(dataKeyForRecord, k -> new ArrayList<>()).add(result.recordsArr[idx]);
        }

        return resultsMap;
    }

    private @NotNull Result<T> getResult(List<K> dataKeys) {
        final int nKeys = dataKeys.size();

        if (!isSingleKeyCol) {
            // Run a sanity check on key components with a clear exception
            final int nCols = keyColumns.length;
            for (K dataKey : dataKeys) {
                final int nKeyComponents = ((List<?>) dataKey).size();
                if (nKeyComponents != nCols) {
                    throw new IllegalArgumentException(
                            "dataKey has " + nKeyComponents + " components; expected " + nCols);
                }
            }
        }

        // Convert data keys (Object or List<?>) to lookup keys (Object or Object[]))
        final List<Object> lookupKeys = dataKeysListToLookupKeys.apply(dataKeys);

        /*
         * Create reference to result arrays. We can't allocate the arrays yet, since for data indexes and partitioned
         * tables we don't know yet how many rows we're going to retrieve data from. Even from a lastBy we don't know
         * how many of the keys are actually present.
         */
        final MutableObject<Object[]> recordDataArrsRef = new MutableObject<>(null);

        // list to store the index keys for which data is retrieved
        final TLongList recordDataRowKeys = new TLongArrayList(nKeys);

        // map of index keys to the position of the corresponding data key (in the 'dataKeys' list)
        final MutableObject<TLongIntMap> dbRowKeyToDataKeyPositionalIndexRef = new MutableObject<>();

        DO_LOCKED_FUNCTION.accept(
                (usePrev) -> retrieveDataMultipleKeys(lookupKeys, recordDataArrsRef, recordDataRowKeys,
                        dbRowKeyToDataKeyPositionalIndexRef, usePrev),
                "KeyedRecordAdapter.getRecords()");

        // Now that we have retrieved a consistent snapshot, create records for each row (turning the columnar
        // data fetched from the engine into user-friendly row data)
        final int nRetrievedRecords = recordDataRowKeys.size();
        final T[] recordsArr =
                multiRowRecordAdapter.createRecordsFromData(recordDataArrsRef.getValue(), nRetrievedRecords);

        return new Result<>(recordDataRowKeys, dbRowKeyToDataKeyPositionalIndexRef.getValue(), nRetrievedRecords,
                recordsArr);
    }

    private static class Result<T> {
        public final TLongList recordDataRowKeys;
        public final TLongIntMap dbRowKeyToDataKeyPositionalIndex;
        public final int nRetrievedRecords;
        public final T[] recordsArr;

        public Result(TLongList recordDataRowKeys,
                TLongIntMap dbRowKeyToDataKeyPositionalIndex,
                int nRetrievedRecords,
                T[] recordsArr) {
            this.recordDataRowKeys = recordDataRowKeys;
            this.dbRowKeyToDataKeyPositionalIndex = dbRowKeyToDataKeyPositionalIndex;
            this.nRetrievedRecords = nRetrievedRecords;
            this.recordsArr = recordsArr;
        }
    }

    /**
     * @param lookupKeys The keys to look up in the ToMapListener
     * @param recordDataArrsRef Reference to be populated with an array that will be filled with arrays of data for each
     *        column
     * @param recordDataKeys A list that will be populated with keys for
     * @param rowKeyToDataKeyPositionRef Map of row keys to the position (in the {@code lookupKeys} list) of the
     *        corresponding data key.
     * @return {@code true} If the operation succeeds and {@code recordDataArrs}, {@code recordDataKeys}, and
     *         {@code rowKeyToDataKeyPositionRef} have been populated.
     */
    protected final boolean retrieveDataMultipleKeys(
            @NotNull final List<Object> lookupKeys,
            @NotNull final MutableObject<Object[]> recordDataArrsRef,
            @NotNull final TLongList recordDataKeys,
            @NotNull final MutableObject<TLongIntMap> rowKeyToDataKeyPositionRef,
            final boolean usePrev) {

        // TODO: it would be helpful if we could also extract each row's position within the table when necessary.

        if (TRACE_LOGGING) {
            Function<List<Object>, String> lookupKeyPrinter = keys -> keys.stream()
                    .map(o -> {
                        if (o instanceof Object[]) {
                            return Arrays.deepToString((Object[]) o);
                        } else {
                            return Objects.toString(o);
                        }
                    })
                    .collect(Collectors.joining(", "));

            System.out.println(
                    "----------\n" +
                            "clock: " + ExecutionContext.getContext().getUpdateGraph().clock().currentStep() + "\n" +
                            "lookupKeys: " + lookupKeyPrinter.apply(lookupKeys) + "\n" +
                            "usePrev: " + usePrev);
        }

        // Get the row keys to retrieve data for
        final RowSetForKeysResult dataKeyLookupResult = targetRowSetRetriever.apply(lookupKeys, usePrev);

        final RowSet rowSetAcrossDataKeys = dataKeyLookupResult.rowSet;
        rowKeyToDataKeyPositionRef.setValue(dataKeyLookupResult.rowKeyToDataKeyPositionMap);

        final Object[] recordDataArrs =
                multiRowRecordAdapter.getTableDataArrayRetriever().createDataArrays(rowSetAcrossDataKeys.intSize());
        recordDataArrsRef.setValue(recordDataArrs);

        multiRowRecordAdapter.getTableDataArrayRetriever().fillDataArrays(
                usePrev,
                rowSetAcrossDataKeys,
                recordDataArrs,
                // Store the row keys that correspond to each row in the
                // recordDataKeys list, so we can map the records back to
                // the data keys after all rows have been retrieved:
                recordDataKeys);

        if (TRACE_LOGGING) {
            System.out.println("recordDataArrs (populated): " + Arrays.deepToString(recordDataArrs) + "\n----------");
        }

        return true;
    }

    /**
     * Get a function that maps objects to possibly-reinterpreted objects that can be used with
     * {@link DataIndex.RowKeyLookup}.
     * 
     * @param types The expected types of each of the elements in the array of objects handled by the returned function.
     * @return If {@code types.length == 0}, a Function that takes a zero-length {@code Object[]} and returns a
     *         zero-length {@code Object[]}. If {@code types.length == 1}, a Function that takes a single object as an
     *         argument and returns either the same object or its boxed primitive representation. If
     *         {@code types.length > 1}, a Function that takes an {@code Object[]} of {@code types.length} and returns a
     *         parallel array with the input array's elements or their boxed primitive representations.
     */
    private static Function<Object, Object> getKeyReinterpreter(Class<?>... types) {
        // TODO: is there some standard function that does this? no; just move this to reinterpretutils
        final int expectedLen = types.length;

        // noinspection unchecked
        final Function<Object, Object>[] reinterpretFunctions = new Function[expectedLen];
        for (int ii = 0; ii < expectedLen; ii++) {
            final Class<?> origType = TypeUtils.getBoxedType(types[ii]);

            final Class<?> targetType = ReinterpretUtils.maybeConvertToPrimitiveDataType(origType);
            if (origType == Boolean.class && targetType == byte.class) {
                reinterpretFunctions[ii] = o -> BooleanUtils.booleanAsByte((Boolean) o);
            } else if (origType == Instant.class && targetType == long.class) {
                reinterpretFunctions[ii] = o -> DateTimeUtils.epochNanos((Instant) o);
            } else if (origType == ZonedDateTime.class && targetType == long.class) {
                reinterpretFunctions[ii] = o -> DateTimeUtils.epochNanos((ZonedDateTime) o);
            } else if (origType != targetType) {
                throw new IllegalStateException("Type " + origType + " must be reinterpreted as " + targetType +
                        ", but conversion is not supported");
            } else {
                reinterpretFunctions[ii] = Function.identity();
            }
        }

        if (expectedLen == 0) {
            return input -> {
                Require.eq(((Object[]) input).length, "origObjs.length", 0);
                return ArrayTypeUtils.EMPTY_OBJECT_ARRAY;
            };
        } else if (expectedLen == 1) {
            return input -> reinterpretFunctions[0].apply(input);
        } else {
            return input -> {
                final Object[] origObjs = (Object[]) input;
                Require.eq(origObjs.length, "origObjs.length", expectedLen);
                final Object[] convertedObjs = new Object[expectedLen];
                for (int ii = 0; ii < origObjs.length; ii++) {
                    convertedObjs[ii] = reinterpretFunctions[ii].apply(origObjs[ii]);
                }
                return convertedObjs;
            };
        }
    }

    /**
     * Populates a record with data from the given {@code dataKey}, if {@link #recordKeyDataUpdater} is set.
     *
     * @param record The record to update
     * @param dataKey The data key
     */
    private void updateRecordWithKeyData(@NotNull final T record, @NotNull final K dataKey) {
        if (recordKeyDataUpdater != null) {
            recordKeyDataUpdater.accept(record, dataKey);
        }
    }

    @SuppressWarnings("unused")
    public boolean isSingleKeyCol() {
        return isSingleKeyCol;
    }

    /**
     * Retrieve a row set with the row keys for all rows matching {@code dataKeys}, using the given
     * {@code aggregationRowLookup}.
     * 
     * @param sourceTable The table. Must be the same table the {@code aggregationRowLookup} is from.
     * @param aggregationRowLookup An aggregationRowLookup for the columns corresponding to the elements of the
     *        {@code dataKeys}
     * @param usePrev Whether to use previous values
     * @param dataKeys The data keys to search for
     * @return The row keys corresponding to the given data keys, and a map of those row keys to the position of the
     *         corresponding data key in the {@code dataKeys} list.
     */
    @NotNull
    private RowSetForKeysResult getRowSetForKeysFromAggregatedTable(
            final Table sourceTable,
            final AggregationRowLookup aggregationRowLookup,
            final boolean usePrev,
            final List<?> dataKeys) {
        final int nKeys = dataKeys.size();
        final TLongIntMap rowKeyToDataKeyPositionMap = new TLongIntHashMap(nKeys);

        final TrackingRowSet trackingRowSet = sourceTable.getRowSet();
        final RowSet rowSet = usePrev ? trackingRowSet.prev() : trackingRowSet;

        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        int ii = 0;
        for (Object dataKey : dataKeys) {
            // TODO: don't care about prev here?
            final int k = aggregationRowLookup.get(dataKey);

            // The AggregationRowLookup never forgets the slot for a given data key, so we need to make sure
            // the returned row key is still present in the table's actual RowSet. (Otherwise we could return
            // data that is still in the column sources but not actually part of the table.)
            if (k != aggregationRowLookup.noEntryValue() && rowSet.containsRange(k, k)) {
                builder.addKey(k);
                rowKeyToDataKeyPositionMap.put(k, ii);
            }

            ii++;
        }

        return new RowSetForKeysResult(builder.build(), rowKeyToDataKeyPositionMap);
    }

    /**
     * Retrieve a row set with the row keys for all rows matching {@code dataKeys}, using the given
     * {@code rowKeyLookup}.
     *
     * @param avgRowsPerKey Supplier of the average number of rows per entry in the data index (i.e.
     *        {@code sourceTable.size() / dataIndex.size()})
     * @param rowKeyLookup The row key lookup from the data index
     * @param dataIndexTableRowSetColSource The column source for the RowSet column in the {@link DataIndex#table() data
     *        index's underlying table}
     * @param usePrev Whether to use previous values
     * @param dataKeys The data keys to search for
     * @return The row keys corresponding to the given data keys, and a map of those row keys to the position of the
     *         corresponding data key in the {@code dataKeys} list.
     */
    @NotNull
    private RowSetForKeysResult getRowSetForKeysWithDataIndex(
            final LongSupplier avgRowsPerKey,
            final DataIndex.RowKeyLookup rowKeyLookup,
            final ColumnSource<RowSet> dataIndexTableRowSetColSource,
            final boolean usePrev,
            final List<?> dataKeys) {

        // heuristic for sizing hashmap -- nKeys * avgRowsPerKey
        final int hashMapInitSize = (int) Long.min(Integer.MAX_VALUE, avgRowsPerKey.getAsLong() * dataKeys.size());

        final TLongIntMap rowKeyToDataKeyPositionMap = new TLongIntHashMap(hashMapInitSize);
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        int ii = 0;
        for (final Object dataKey : dataKeys) {
            final long indexTableRowKey = rowKeyLookup.apply(dataKey, usePrev);
            if (indexTableRowKey != RowSequence.NULL_ROW_KEY) {
                final RowSet rowSetForDataKey = usePrev ? dataIndexTableRowSetColSource.getPrev(indexTableRowKey)
                        : dataIndexTableRowSetColSource.get(indexTableRowKey);

                if (rowSetForDataKey == null || rowSetForDataKey.isEmpty()) {
                    ii++;
                    continue;
                }

                // map the row keys back to the corresponding data key, so that after pull the data we know which
                // data key goes with each row of the data
                for (final RowSet.RangeIterator iter = rowSetForDataKey.rangeIterator(); iter.hasNext();) {
                    final long start = iter.next();
                    final long end = iter.currentRangeEnd();
                    for (long k = start; k <= end; k++) {
                        rowKeyToDataKeyPositionMap.put(k, ii);
                    }
                    builder.addRange(start, end);
                }
            }
            ii++;
        }

        return new RowSetForKeysResult(builder.build(), rowKeyToDataKeyPositionMap);
    }

    /**
     * Holder for the result of a {@link RowSetRetriever}. Consists of a {@code rowSet}, and a {code
     * rowKeyToDataKeyPositionMap} that maps each row key in that {@code rowSet} to the position of the key it
     * corresponds to in a list. (The list of keys itself is not part of this object, but is passed to the
     * {@link RowSetRetriever#apply} used to create a {@code RowSetKeysForResult}.)
     */
    private static class RowSetForKeysResult {
        private final RowSet rowSet;
        private final TLongIntMap rowKeyToDataKeyPositionMap;

        private RowSetForKeysResult(final RowSet rowSet, final TLongIntMap rowKeyToDataKeyPositionMap) {
            this.rowSet = rowSet;
            this.rowKeyToDataKeyPositionMap = rowKeyToDataKeyPositionMap;
        }
    }

    /**
     * Interface for lambda that looks up the RowSet corresponding to the keys in hte {@code keysList}.
     */
    @FunctionalInterface
    private interface RowSetRetriever {
        RowSetForKeysResult apply(List<?> keysList, boolean usePrev);
    }
}
