//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongIntMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.util.ToMapListener;
import io.deephaven.dataadapter.consumers.*;
import io.deephaven.dataadapter.datafetch.single.SingleRowRecordAdapter;
import io.deephaven.dataadapter.locking.GetDataLockType;
import io.deephaven.dataadapter.locking.QueryDataRetrievalOperation;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import io.deephaven.dataadapter.rec.updaters.*;
import io.deephaven.util.function.ThrowingBiConsumer;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Adapter for using {@link io.deephaven.engine.table.Table Tables} like hashmaps.
 * <p>
 * Instantiate through {@link #makeKeyedRecordAdapterSimpleKey}/{@link #makeRecordAdapterCompositeKey}.
 *
 * @param <K> Key type (either some object or a {@code List<?>}). If {@code K} is a {@code List<?>}, then a
 *        {@link #tupleSource} appropriate for the {@link #keyColumns}' types will be used to generate keys for the
 *        inner {@link #toMapListener}. If {@code K} is <b>any type except</b> a {@code List<?>}, then {@code K} will be
 *        used directly as the {@code toMapListener}'s key type (and must match the data type in the {@code keyColumn}).
 * @param <T> Record type
 */
public class KeyedRecordAdapter<K, T> {

    /**
     * Consumer that obtains lock before calling code
     */
    @NotNull
    protected final ThrowingBiConsumer<QueryDataRetrievalOperation, String, RuntimeException> DO_LOCKED_FUNCTION;

    /**
     * The ToMapListener that tracks the DH index for each key. Table data is never retrieved through the ToMapListener.
     */
    @NotNull
    private final ToMapListener<Object, Void> toMapListener;

    /**
     * A source of tuples to use for composite map keys. Will be {@code null} if there is only one {@link #keyColumns
     * key column}.
     */
    @Nullable
    private final TupleSource<?> tupleSource;

    /**
     * Function used when converting user-friendly keys to the ToMapListener's keys. The keys must be either an
     * {@code Object} (if there is one {@link #keyColumns key column}) or a {@code List<?>} (if there are multiple key
     * columns).
     */
    @NotNull
    private final Function<K, Object> dataKeyToMapKey;

    /**
     * Function convert a list of user-friendly keys to a list of the ToMapListener's keys. The user-friendly keys must
     * be either a {@code List<K>} (if there is one {@link #keyColumns key column}) or a {@code List<List<?>>} (if there
     * are multiple key columns).
     * <p>
     * This function will call {@link #dataKeyToMapKey}; if there are multiple key columns, the keys must first be
     * converted from a {@code List<?>} to an {@code Object[]}.
     */
    @NotNull
    protected final Function<List<?>, List<Object>> dataKeysListToMapKeys;

    /**
     * The keys by which records are identified.
     */
    @NotNull
    private final String[] keyColumns;

    @NotNull
    protected final SingleRowRecordAdapter<T> singleRowRecordAdapter;

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
    private final RecordAdapterDescriptor<T> rowRecordAdapterDescriptor;

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
        if (keyColumns.length == 0) {
            throw new IllegalArgumentException("At least one key column must be provided!");
        }

        this.keyColumns = keyColumns;
        this.rowRecordAdapterDescriptor = rowRecordAdapterDescriptor;

        isSingleKeyCol = keyColumns.length == 1;

        final ColumnSource<?>[] keyColumnSources =
                Arrays.stream(keyColumns).map(sourceTable::getColumnSource).toArray(ColumnSource[]::new);

        // use tuples as ToMapListener keys
        tupleSource = isSingleKeyCol ? null : TupleSourceFactory.makeTupleSource(keyColumnSources);
        toMapListener = ToMapListener.make(
                sourceTable,
                isSingleKeyCol ? keyColumnSources[0]::get : tupleSource::createTuple,
                isSingleKeyCol ? keyColumnSources[0]::getPrev : tupleSource::createPreviousTuple,
                // no value providers (value type is Void; listener is only used for tracking keys)
                null,
                null);
        sourceTable.addUpdateListener(toMapListener);

        // TODO: should this be the the listener instead of the sourceTable? probably? bit confusing with
        // ToMapListener's baselineMap/currentMap stuff.
        // I bet it's specifically broken to introduce the notification-awareness but
        // to only pay attention to the table.
        final NotificationStepSource notificationSource =
                sourceTable instanceof BaseTable ? (BaseTable) sourceTable : null;
        // final NotificationStepSource notificationSource = toMapListener;
        DO_LOCKED_FUNCTION = GetDataLockType.getDoLockedConsumer(
                sourceTable.getUpdateGraph(),
                GetDataLockType.SNAPSHOT,
                notificationSource);


        // Create a record adapter that excludes the key columns, if they are present, (since all available keys will
        // already be in memory)
        final RecordAdapterDescriptorBuilder<T> recordAdapterDescriptorBuilderNoKeys =
                RecordAdapterDescriptorBuilder.create(rowRecordAdapterDescriptor);

        // Remove the key columns from the new record adapter descriptor and put them in a map.
        final Map<String, RecordUpdater<T, ?>> keyColRecordUpdaters = new LinkedHashMap<>();
        for (String keyColName : keyColumns) {
            final RecordUpdater<T, ?> keyColRecordUpdater =
                    recordAdapterDescriptorBuilderNoKeys.removeColumn(keyColName);
            if (keyColRecordUpdater != null) {
                keyColRecordUpdaters.put(keyColName, keyColRecordUpdater);
            }
        }

        final RecordAdapterDescriptor<T> recordAdapterDescriptorNoKeys = recordAdapterDescriptorBuilderNoKeys.build();

        singleRowRecordAdapter = recordAdapterDescriptorNoKeys.createSingleRowRecordAdapter(sourceTable);
        multiRowRecordAdapter = recordAdapterDescriptorNoKeys.createMultiRowRecordAdapter(sourceTable);

        final Class<?>[] keyColumnTypes =
                Arrays.stream(keyColumnSources).map(ColumnSource::getType).toArray(Class[]::new);
        keyColumnTypesList = Collections.unmodifiableList(Arrays.asList(keyColumnTypes));

        // Set the dataKeyToMapKey, dataKeysListToMapKeys, and updateRecordWithKeyData lambdas depending on
        // whether the key is a simple key from a single column or a composite key from multiple columns:
        if (isSingleKeyCol) {
            /*
             * the contents of the dataKeys list are the map keys themselves. dataKeys is the list of keys that we want
             * to look up in the ToMapListener.
             */
            // noinspection unchecked
            dataKeyToMapKey = (Function<K, Object>) Function.identity();

            // a list of keys passed to getRecords() will be just that -- a List<K>
            dataKeysListToMapKeys = dataKeys -> {
                // noinspection unchecked
                return (List<Object>) dataKeys;
            };

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
            // the contents of the datakeys list are composite keys (i.e. datakeys list
            // is a List<List<?>>), which must be converted to tuples.
            // mapKeys is the list of keys (tuples) that we want to look up in the ToMapListener.
            dataKeyToMapKey = o -> compositeKeyToMapKey((List<?>) o);

            // a list of keys passed to getRecords() will be a List<List<?>>; need extract
            // arrays from the inner lists before letting dataKeyToMapKey create tuples.
            dataKeysListToMapKeys = dataKeys -> {
                final List<Object> mapKeys = new ArrayList<>(dataKeys.size());
                for (final Object dataKey : dataKeys) {
                    // noinspection unchecked
                    final Object keyTuple = dataKeyToMapKey.apply((K) dataKey);
                    mapKeys.add(keyTuple);
                }
                return mapKeys;
            };


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

    public static <T> KeyedRecordAdapter<List<?>, T> makeRecordAdapterCompositeKey(Table sourceTable,
            RecordAdapterDescriptor<T> recordAdapterDescriptor, String... keyColumns) {
        return new KeyedRecordAdapter<>(sourceTable, recordAdapterDescriptor, keyColumns);
    }

    public static <K, T> KeyedRecordAdapter<K, T> makeKeyedRecordAdapterSimpleKey(Table sourceTable,
            RecordAdapterDescriptor<T> recordAdapterDescriptor, String keyColumn, Class<K> keyColType) {
        // throw an exception if keyColumn is not of keyColType:
        sourceTable.getColumnSource(keyColumn, TypeUtils.getUnboxedTypeIfBoxed(keyColType));
        return new KeyedRecordAdapter<>(sourceTable, recordAdapterDescriptor, keyColumn);
    }

    @NotNull
    private Object compositeKeyToMapKey(@NotNull final List<?> dataKey) {
        Assert.neqNull(tupleSource, "tupleSource");

        final int nCols = keyColumns.length;
        final int nKeyComponents = dataKey.size();
        if (nKeyComponents != nCols) {
            throw new IllegalArgumentException("dataKey has " + nKeyComponents + " components; expected " + nCols);
        }

        // noinspection ConstantConditions
        return tupleSource.createTupleFromValues(dataKey.toArray());
    }

    /**
     * Retrieve records of type {@code T} corresponding to the data in the table for the given {@code dataKeys}.
     */
    public Map<K, T> getRecords(final List<K> dataKeys) {
        final int nKeys = dataKeys.size();

        // Convert data keys (object or List<?>) to map keys (object or tuple)
        final List<Object> mapKeys = dataKeysListToMapKeys.apply(dataKeys);

        // create arrays to hold the result data
        final Object[] recordDataArrs = multiRowRecordAdapter.getTableDataArrayRetriever().createDataArrays(nKeys);

        // list to store the index keys for which data is retrieved
        final TLongList recordDataRowKeys = new TLongArrayList(nKeys);

        // map of index keys to the position of the corresponding data key (in the 'dataKeys' list)
        final MutableObject<TLongIntMap> dbRowKeyToDataKeyPositionalIndexRef = new MutableObject<>();

        DO_LOCKED_FUNCTION.accept(
                (usePrev) -> retrieveDataMultipleKeys(mapKeys, recordDataArrs, recordDataRowKeys,
                        dbRowKeyToDataKeyPositionalIndexRef, usePrev),
                "KeyedRecordAdapter.getRecords()");

        final int nRetrievedRecords = recordDataRowKeys.size();

        // Now that we have retrieved a consistent snapshot, create records for each row
        final TLongIntMap dbIdxKeyToDataKeyPositionalIndex = dbRowKeyToDataKeyPositionalIndexRef.getValue();
        final Map<K, T> resultsMap = new HashMap<>(nRetrievedRecords);
        final T[] recordsArr = multiRowRecordAdapter.createRecordsFromData(recordDataArrs, nRetrievedRecords);
        for (int idx = 0; idx < nRetrievedRecords; idx++) {
            // Find the data key corresponding to the DB index key from
            // which this record was retrieved. Map the data key to the record.
            final long idxKeyForRecord = recordDataRowKeys.get(idx);
            final int dataKeyIdxForRecord = dbIdxKeyToDataKeyPositionalIndex.get(idxKeyForRecord);
            final K dataKeyForRecord = dataKeys.get(dataKeyIdxForRecord);

            // If the key columns are included in the record, then update them based
            // on the data key (without reading from DB)
            updateRecordWithKeyData(recordsArr[idx], dataKeyForRecord);

            resultsMap.put(dataKeyForRecord, recordsArr[idx]);
        }

        return resultsMap;
    }

    @SuppressWarnings("unchecked")
    public Map<K, T> getRecords(final K... dataKeys) {
        return getRecords(Arrays.asList(dataKeys));
    }

    /**
     * @param mapKeys The keys to look up in the ToMapListener
     * @param recordDataArrs Array that will be filled with data for each column
     * @param recordDataKeys A list that will be populated with keys for
     * @param dbIdxKeyToDataKeyPositionRef Map of keys
     * @param usePrevOrig Whether to look at prev values when retrieving data. This is from ConstructSnapshot. It is not
     *        needed but is checked against {@link ToMapListener.RowSetForKeysResult#usePrev} for consistency.
     * @return {@code true} If the operation succeeds and {@code recordDataArrs}, {@code recordDataKeys}, and
     *         {@code dbIdxKeyToDataKeyPositionRef} have been populated.
     */
    protected final boolean retrieveDataMultipleKeys(
            @NotNull final List<Object> mapKeys,
            @NotNull final Object[] recordDataArrs,
            @NotNull final TLongList recordDataKeys,
            @NotNull final MutableObject<TLongIntMap> dbIdxKeyToDataKeyPositionRef,
            final boolean usePrevOrig) {
        final ToMapListener.RowSetForKeysResult keyToIndexResult = toMapListener.getRowSetForKeys(mapKeys);

        final RowSet tableIndex = keyToIndexResult.rowSet;
        dbIdxKeyToDataKeyPositionRef.setValue(keyToIndexResult.idxKeyToDataKeyPositionMap);

        final boolean usePrev = keyToIndexResult.usePrev;

        // TODO: probably we should just return false here, since it means LTM has ticked and snapshot will not be
        // consistent?
        Assert.eq(usePrevOrig, "usePrevOrig", usePrevOrig, "keyToIndexResult.usePrev");

        multiRowRecordAdapter.getTableDataArrayRetriever().fillDataArrays(
                usePrev,
                tableIndex,
                recordDataArrs,
                // Note the index keys that correspond to each row in the
                // recordDataKeys list, so we can map the records back to
                // the data keys after all rows have been retrieved:
                recordDataKeys::add);

        return true;
    }

    /**
     * Retrieve a record of data.
     *
     * @param dataKey The key for which to retrieve data
     * @return A record containing data for the key, if the key is present in the table. Otherwise, {@code null}.
     */
    public T getRecord(K dataKey) {
        final Object mapKey = dataKeyToMapKey.apply(dataKey);
        final T result = singleRecordLockedRetriever(mapKey);
        if (result != null) {
            updateRecordWithKeyData(result, dataKey);
        }
        return result;
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

    private T singleRecordLockedRetriever(final Object mapKey) {
        final MutableObject<T> result = new MutableObject<>();
        DO_LOCKED_FUNCTION.accept(
                usePrev -> {
                    result.setValue(toMapListener.get(
                            mapKey,
                            k -> singleRowRecordAdapter.retrieveDataSingleKey(k, false),
                            k -> singleRowRecordAdapter.retrieveDataSingleKey(k, true)));
                    return true;
                }, "KeyedRecordAdapter.getRecord()");
        return result.getValue();
    }

    @SuppressWarnings("unused")
    public boolean isSingleKeyCol() {
        return isSingleKeyCol;
    }
}
