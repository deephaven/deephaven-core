//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.dataindex.TableBackedDataIndex;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.dataadapter.consumers.*;
import io.deephaven.dataadapter.locking.GetDataLockType;
import io.deephaven.dataadapter.locking.QueryDataRetrievalOperation;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import io.deephaven.dataadapter.rec.updaters.*;
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
import java.util.stream.Collectors;

/**
 * Adapter for using {@link io.deephaven.engine.table.Table Tables} like hashmaps.
 * <p>
 * Instantiate through {@link #makeRecordAdapterSimpleKey}/{@link #makeRecordAdapterCompositeKey}.
 *
 * @param <K> Key type (either some object or a {@code List<?>}). If {@code K} is a {@code List<?>}, then a
 *        {@link #tupleSource} appropriate for the {@link #keyColumns}' types will be used to generate keys for the
 *        inner {@link #toMapListener}. If {@code K} is <b>any type except</b> a {@code List<?>}, then {@code K} will be
 *        used directly as the {@code toMapListener}'s key type (and must match the data type in the {@code keyColumn}).
 * @param <T> Record type
 */
// MAJOR TODOs: how to handle returning multiple rows for a given key?
public class KeyedRecordAdapter<K, T> {

    // TODO: remove this & the logging
    private final boolean TRACE_LOGGING = true;

    /**
     * Consumer that obtains lock before calling code
     */
    @NotNull
    protected final ThrowingBiConsumer<QueryDataRetrievalOperation, String, RuntimeException> DO_LOCKED_FUNCTION;

    /**
     * Function convert a list of user-friendly keys to a list of the ToMapListener's keys. The user-friendly keys must
     * be either a {@code List<K>} (if there is one {@link #keyColumns key column}) or a {@code List<List<?>>} (if there
     * are multiple key columns).
     * <p>
     * This function will call {@link #dataKeyToLookupKey}; if there are multiple key columns, the keys must first be
     * converted from a {@code List<?>} to an {@code Object[]}.
     */
    @NotNull
    protected final Function<List<?>, List<Object>> dataKeysListToLookupKeys;

    /**
     * The keys by which records are identified.
     */
    @NotNull
    private final String[] keyColumns;

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
    protected final List<Class<?>> keyColumnTypesList;

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
     *
     * @param theTable
     * @param dataIndex
     * @param rowRecordAdapterDescriptor
     * @param keyColumns
     */
    private KeyedRecordAdapter(final Table theTable,
            final PartitionedTable thePartitionedTable,
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
            // TODO: assert that the dataIndex is actually on 'theTable'?
        } else if (thePartitionedTable != null) {
            // TODO: for partitioned tables we need to worry about constituentChangesPermitted, which is independent of refreshingness of the constituent
            sourceTable = thePartitionedTable.table();
            // make sure we have the expected keys
            Arrays.asList(keyColumns).forEach(colName -> Require.contains(
                    thePartitionedTable.table().getDefinition().getColumnNameSet(),
                    "thePartitionedTable.table().getDefinition().getColumnNameSet()",
                    colName,
                    "colName"));
        } else {
            sourceTable = theTable.lastBy(keyColumns);
        }

        isSingleKeyCol = keyColumns.length == 1;

        final ColumnSource<?>[] keyColumnSources =
                Arrays.stream(keyColumns)
                        .map(sourceTable::getColumnSource)
                        .map(ReinterpretUtils::maybeConvertToPrimitive)
                        .toArray(ColumnSource[]::new);

        if (dataIndex != null) {
            final DataIndex.RowKeyLookup rowKeyLookup = dataIndex.rowKeyLookup(keyColumnSources);
            targetRowSetRetriever = (keysList, usePrev) -> getRowSetForKeysWithDataIndex(
                    rowKeyLookup,
                    dataIndex.table().getColumnSource(dataIndex.rowSetColumnName()),
                    usePrev,
                    keysList);
        } else if (sourceTable.hasAttribute(Table.AGGREGATION_ROW_LOOKUP_ATTRIBUTE)) {
            targetRowSetRetriever = (keysList, usePrev) -> getRowSetForKeysFromAggregatedTable(
                    AggregationProcessor.getRowLookup(sourceTable),
                    usePrev,
                    keysList);
        } else {
            // TODO: partitioned table lookup? also, note that *the partitioned table depends on its constituents*
            /* special case of the above, since a partitioned table (in the context we care about) is an aggregation */

            targetRowSetRetriever = (keysList, usePrev) -> getRowSetForKeysFromPartitionedTable(
                    AggregationProcessor.getRowLookup(sourceTable),
                    sourceTable.getColumnSource(thePartitionedTable.constituentColumnName(), Table.class),
                    usePrev,
                    keysList
            );

            /* filter the PartitionedTable.table(), then iterate over the constituents and rip out all the rows of all those tables. */

            throw new UnsupportedOperationException();
        }
        // TODO: use io.deephaven.engine.table.impl.by.AggregationRowLookup.get. But read its documentation! e.g. about
        // reinterpreting. you can get this for any aggregation result
        // To get teh AggregationRowLookup, use io.deephaven.engine.table.impl.by.AggregationProcessor.getRowLookup.
        // Call it on the exact table you're going to get data from (ie. the lastBy result itself).
        // Also, might need to do some magic to make sure this thing is turned on. (Making a ContextFactory??)
        // TODO: ticket about tracking the key columns of a table? or rather, 'automatically add data indexes to agg
        // results'


        // TODO: for also look at io.deephaven.engine.table.impl.dataindex.TableBackedDataIndex.computeTable /
        // io.deephaven.engine.table.impl.dataindex.TableBackedDataIndex.rowKeyLookup

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

        // Get a record adapter descriptor that deals with the raw data (without the keys -- since they can be populated afterward)
        final RecordAdapterDescriptor<T> recordAdapterDescriptorNoKeys = recordAdapterDescriptorBuilderNoKeys.build();

        if (thePartitionedTable != null) {
            multiRowRecordAdapter = recordAdapterDescriptorNoKeys.createMultiRowRecordAdapter(thePartitionedTable);
        } else {
            multiRowRecordAdapter = recordAdapterDescriptorNoKeys.createMultiRowRecordAdapter(sourceTable);
        }

        final Class<?>[] keyColumnTypes =
                Arrays.stream(keyColumnSources).map(ColumnSource::getType).toArray(Class[]::new);
        keyColumnTypesList = List.of(keyColumnTypes);

        // Function for converting a lookup key to the corresponding reinterpreted types.
        final Function<Object, Object> dataKeyReinterpreter = getHackyReinterpreter(keyColumnTypes);

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
                            "Key column 0: Expected type " + expectedType + ", instead found type " +
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
                    final Class<?> expectedType = keyColRecordUpdater.getSourceType();
                    if (!expectedType.isAssignableFrom(keyColumnTypes[i])) {
                        throw new IllegalArgumentException(
                                "Key column " + i + ": Expected type " + expectedType + ", instead found type " +
                                        keyColumnSources[i].getType().getCanonicalName());
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
            throw new IllegalArgumentException("Attempting to create composite-key KeyedRecordAdapter but dataIndex has only one key column. Use makeRecordAdapterSimpleKey instead.");
        }

        final String[] keyColumns = keyColumnsList.toArray(new String[0]);
        return new KeyedRecordAdapter<>(sourceTable, null, dataIndex, recordAdapterDescriptor, keyColumns);
    }


    public static <T> KeyedRecordAdapter<List<?>, T> makeRecordAdapterCompositeKey(PartitionedTable sourceTable,
                                                                                   RecordAdapterDescriptor<T> recordAdapterDescriptor) {
        final List<String> keyColumnsList = new ArrayList<>(sourceTable.keyColumnNames());

        if (keyColumnsList.isEmpty()) {
            throw new IllegalArgumentException("dataIndex has no key columns!");
        }
        if (keyColumnsList.size() == 1) {
            throw new IllegalArgumentException("Attempting to create composite-key KeyedRecordAdapter but dataIndex has only one key column. Use makeRecordAdapterSimpleKey instead.");
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
            throw new IllegalArgumentException("Attempting to create simple-key KeyedRecordAdapter but dataIndex has multiple key columns. Use makeRecordAdapterCompositeKey instead.");
        }

        // Ensure the key column type matches between the given keyColType and the dataIndex
        final String colNameInDataIndex = keyColumnsList.get(0);
        final Class<Object> dataIndexKeyColType = dataIndex.table().getColumnSource(colNameInDataIndex).getType();
        if (!keyColType.isAssignableFrom(dataIndexKeyColType)) {
            throw new IllegalArgumentException("Key column type mismatch: expected type " + keyColType.getName() + ", found " + dataIndexKeyColType.getName() + " in dataIndex for column " + colNameInDataIndex);
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
            throw new IllegalArgumentException("Attempting to create simple-key KeyedRecordAdapter but dataIndex has multiple key columns. Use makeRecordAdapterCompositeKey instead.");
        }

        // Ensure the key column type matches between the given keyColType and the dataIndex
        final String colNameInDataIndex = keyColumnsList.get(0);
        final Class<Object> dataIndexKeyColType = sourceTable.table().getColumnSource(colNameInDataIndex).getType();
        if (!keyColType.isAssignableFrom(dataIndexKeyColType)) {
            throw new IllegalArgumentException("Key column type mismatch: expected type " + keyColType.getName() + ", found " + dataIndexKeyColType.getName() + " in partitioned table for column " + colNameInDataIndex);
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
     * Retrieve a record of type {@code T} corresponding to the data in the table for the given {@code dataKey}. If
     * the table contains multiple records for the given key, the last record will be returned.
     *
     * @param dataKey The key for which to retrieve data
     * @return A record containing data for the key, if the key is present in the table. Otherwise, {@code null}.
     */
    public T getRecord(K dataKey) {
        return getRecords(dataKey).get(dataKey);
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
     * Retrieve multiple records of type {@code T} corresponding to the data in the table for the given {@code dataKey}. If
     * the table contains multiple records for the given key, the last record will be returned.
     *
     * @param dataKey The key for which to retrieve data
     * @return A List of record containing data for the key, if the key is present in the table. Otherwise, {@code null}.
     */
    public List<T> getRecordList(K dataKey) {
        return getRecordsLists(dataKey).get(dataKey);
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

        // Now that we have retrieved a consistent snapshot, create records for each row
        final TLongIntMap dbIdxKeyToDataKeyPositionalIndex = result.dbRowKeyToDataKeyPositionalIndexRef.getValue();
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

        // Now that we have retrieved a consistent snapshot, create records for each row
        final TLongIntMap dbIdxKeyToDataKeyPositionalIndex = result.dbRowKeyToDataKeyPositionalIndexRef.getValue();
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

        if(!isSingleKeyCol) {
            // Run a sanity check on key components with a clear exception
            final int nCols = keyColumns.length;
            for (K dataKey : dataKeys) {
                final int nKeyComponents = ((List<?>) dataKey).size();
                if (nKeyComponents != nCols) {
                    throw new IllegalArgumentException("dataKey has " + nKeyComponents + " components; expected " + nCols);
                }
            }
        }

        // Convert data keys (Object or List<?>) to lookup keys (Object or Object[]))
        final List<Object> lookupKeys = dataKeysListToLookupKeys.apply(dataKeys);

        // create arrays to hold the result data
        /* TODO: for data indexes/partitioned tables, we don't actually know how many rows we're pulling data for, so that sucks because it means I need to create the arrays under the snapshot rather than before taking a lock, but who really cares */
        final Object[] recordDataArrs = multiRowRecordAdapter.getTableDataArrayRetriever().createDataArrays(nKeys);

        // list to store the index keys for which data is retrieved
        final TLongList recordDataRowKeys = new TLongArrayList(nKeys);

        // map of index keys to the position of the corresponding data key (in the 'dataKeys' list)
        final MutableObject<TLongIntMap> dbRowKeyToDataKeyPositionalIndexRef = new MutableObject<>();

        DO_LOCKED_FUNCTION.accept(
                (usePrev) -> retrieveDataMultipleKeys(lookupKeys, recordDataArrs, recordDataRowKeys,
                        dbRowKeyToDataKeyPositionalIndexRef, usePrev),
                "KeyedRecordAdapter.getRecords()");

        final int nRetrievedRecords = recordDataRowKeys.size();
        final T[] recordsArr = multiRowRecordAdapter.createRecordsFromData(recordDataArrs, nRetrievedRecords);
        Result result = new Result(recordDataRowKeys, dbRowKeyToDataKeyPositionalIndexRef, nRetrievedRecords, recordsArr);
        return result;
    }

    private static class Result<T> {
        public final TLongList recordDataRowKeys;
        public final MutableObject<TLongIntMap> dbRowKeyToDataKeyPositionalIndexRef;
        public final int nRetrievedRecords;
        public final T[] recordsArr;

        public Result(TLongList recordDataRowKeys, MutableObject<TLongIntMap> dbRowKeyToDataKeyPositionalIndexRef, int nRetrievedRecords, T[] recordsArr) {
            this.recordDataRowKeys = recordDataRowKeys;
            this.dbRowKeyToDataKeyPositionalIndexRef = dbRowKeyToDataKeyPositionalIndexRef;
            this.nRetrievedRecords = nRetrievedRecords;
            this.recordsArr = recordsArr;
        }
    }

    /**
     * @param lookupKeys The keys to look up in the ToMapListener
     * @param recordDataArrs Array that will be filled with data for each column
     * @param recordDataKeys A list that will be populated with keys for
     * @param rowKeyToDataKeyPositionRef Map of row keys to the position (in the {@code lookupKeys} list) of the
     *        corresponding data key.
     * @return {@code true} If the operation succeeds and {@code recordDataArrs}, {@code recordDataKeys}, and
     *         {@code rowKeyToDataKeyPositionRef} have been populated.
     */
    protected final boolean retrieveDataMultipleKeys(
            @NotNull final List<Object> lookupKeys,
            @NotNull final Object[] recordDataArrs,
            @NotNull final TLongList recordDataKeys,
            @NotNull final MutableObject<TLongIntMap> rowKeyToDataKeyPositionRef,
            final boolean usePrev) {


        if (TRACE_LOGGING) {
            Function<List<Object>, String> lookupKeyPrinter = keys -> keys.stream()
                    .map(o -> { if (o instanceof Object[]) { return Arrays.deepToString((Object[]) o); } else { return Objects.toString(o); } })
                    .collect(Collectors.joining(", "));

            System.out.println(
                    "clock: " + ExecutionContext.getContext().getUpdateGraph().clock().currentStep() + "\n" +
                    "lookupKeys: " + lookupKeyPrinter.apply(lookupKeys) + "\n" +
                    "usePrev: " + usePrev);
        }

        // Get the row keys to retrieve data for
        final RowSetForKeysResult dataKeyLookupResult = targetRowSetRetriever.apply(lookupKeys, usePrev);

        final RowSet rowSetAcrossDataKeys = dataKeyLookupResult.rowSet;
        rowKeyToDataKeyPositionRef.setValue(dataKeyLookupResult.rowKeyToDataKeyPositionMap);

        final Object[] arrsForAllRows = multiRowRecordAdapter.getTableDataArrayRetriever().createDataArrays(rowSetAcrossDataKeys.intSize());
        Assert.eq(recordDataArrs.length, "recordDataArrs.length", arrsForAllRows.length, "arrsForAllRows.length");

        // TODO: by sizing the recordDataArrs based on the size of the row set, we won't necessarily have the same length arrs as before,
        //   since we won't have slots for keys we didn't find....?? how was this handled before??
        System.arraycopy(arrsForAllRows, 0, recordDataArrs, 0, recordDataArrs.length);

        multiRowRecordAdapter.getTableDataArrayRetriever().fillDataArrays(
                usePrev,
                rowSetAcrossDataKeys,
                recordDataArrs,
                // Store the row keys that correspond to each row in the
                // recordDataKeys list, so we can map the records back to
                // the data keys after all rows have been retrieved:
                recordDataKeys);

        if (TRACE_LOGGING) System.out.println("recordDataArrs (populated): " + Arrays.deepToString(recordDataArrs));

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
    private static Function<Object, Object> getHackyReinterpreter(Class<?>... types) {
        // TODO: is there some standard function that does this? no; just move this to reinterpretutils
        final int expectedLen = types.length;

        // noinspection unchecked
        final Function<Object, Object>[] reinterpretFunctions = new Function[expectedLen];
        for (int ii = 0; ii < expectedLen; ii++) {
            final Class<?> origType = TypeUtils.getBoxedType(types[ii]);

            final Class<?> targetType = ReinterpretUtils.maybeConvertToPrimitiveDataType(origType);
            if (origType == Boolean.class && targetType == byte.class) {
                Require.equals(targetType, "targetType", Boolean.class);
                reinterpretFunctions[ii] = o -> BooleanUtils.booleanAsByte((Boolean) o);
            } else if (origType == Instant.class && targetType == long.class) {
                Require.equals(targetType, "targetType", Instant.class);
                reinterpretFunctions[ii] = o -> DateTimeUtils.epochNanos((Instant) o);
            } else if (origType == ZonedDateTime.class && targetType == long.class) {
                Require.equals(targetType, "targetType", ZonedDateTime.class);
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

    @NotNull
    private RowSetForKeysResult getRowSetForKeysFromAggregatedTable(
            final AggregationRowLookup aggregationRowLookup,
            final boolean usePrev,
            final List<?> dataKeys) {
        final int nKeys = dataKeys.size();
        final TLongIntMap rowKeyToDataKeyPositionMap = new TLongIntHashMap(nKeys);

        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        int ii = 0;
        for (Object dataKey : dataKeys) {
            // TODO: don't care about prev here?
            final int k = aggregationRowLookup.get(dataKey);

            if (k != aggregationRowLookup.noEntryValue()) {
                builder.addKey(k);
                rowKeyToDataKeyPositionMap.put(k, ii);
            }

            ii++;
        }

        return new RowSetForKeysResult(builder.build(), rowKeyToDataKeyPositionMap);
    }

    @NotNull
    private RowSetForKeysResult getRowSetForKeysWithDataIndex(
            final DataIndex.RowKeyLookup rowKeyLookup,
            final ColumnSource<RowSet> dataIndexTableRowSetColSource,
            final boolean usePrev,
            final List<?> dataKeys) {
        final int nKeys = dataKeys.size();
        final TLongIntMap rowKeyToDataKeyPositionMap = new TLongIntHashMap(nKeys);
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        int ii = 0;
        for (Object dataKey : dataKeys) {
            final long indexTableRowKey = rowKeyLookup.apply(dataKey, usePrev);
            if (indexTableRowKey != RowSequence.NULL_ROW_KEY) {
                final RowSet rowSetForDataKey = usePrev ? dataIndexTableRowSetColSource.getPrev(indexTableRowKey)
                        : dataIndexTableRowSetColSource.get(indexTableRowKey);

                if (rowSetForDataKey == null || rowSetForDataKey.isEmpty())
                    continue;

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

                /*
                get me data for AAPL, GOOG
                0 1
                0 is GOOG
                1 is AAPL



                AAPL: 0-100, 300-399
                GOOG: 101-200

                result rowset to retrieve for: 0-200, 300-399

                AAPL is dataKey 0, GOOG is dataKey 1

                rowKeyToDatakeyPostionMap:

                0 -> 0
                1 -> 0
                2 -> 0
                ...



                 */
            }
            ii++;
        }

        return new RowSetForKeysResult(builder.build(), rowKeyToDataKeyPositionMap);
    }

    private RowSetForKeysResult getRowSetForKeysFromPartitionedTable(
            final AggregationRowLookup aggregationRowLookup,
            final ColumnSource<Table> partitionedTableConstituentColSource,
            final boolean usePrev,
            final List<?> dataKeys) {
        // TODO: this is a bit of a trickier case because the actual data I'm getting is from a zillion different tables
        // (each constituent)...which potentially means the SnapshotControl has more/different sources to consider?
        if (true) {
            throw new UnsupportedOperationException("method not yet implemented");
        }
        final int nKeys = dataKeys.size();
        PartitionedTable x;

        // TODO: use constituentFor(), or do a straight-up where() on the PartitionedTable.table(), or use the AggregationRowLookup (assuming it was produced with .partitionBy() rather than being a source table)

        final TLongIntMap rowKeyToDataKeyPositionMap = new TLongIntHashMap(nKeys);
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        int ii = 0;

        for (Object dataKey : dataKeys) {
            final long constituentRowKey = aggregationRowLookup.get(dataKey);
            if (constituentRowKey != RowSequence.NULL_ROW_KEY) {
                builder.addKey(constituentRowKey);
                rowKeyToDataKeyPositionMap.put(constituentRowKey, ii);
            }
            ii++;
        }

        return new RowSetForKeysResult(builder.build(), rowKeyToDataKeyPositionMap);
    }


    /**
     * Holder for the result of a {@link RowSetRetriever}. Consists of a {@code rowSet}, and a
     * {code rowKeyToDataKeyPositionMap} that maps each row key in that {@code rowSet} to the position of the key it
     * corresponds to in a list. (The list of keys itself is not part of this object, but is passed to the {@link RowSetRetriever#apply}
     * used to create a {@code RowSetKeysForResult}.)
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
