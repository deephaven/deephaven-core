//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongIntMap;
import io.deephaven.base.verify.Require;
import io.deephaven.dataadapter.datafetch.bulk.DefaultMultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.updaters.ObjRecordUpdater;
import io.deephaven.dataadapter.rec.updaters.RecordUpdater;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Python-facing interface to {@code KeyedRecordAdapter}.
 */
@SuppressWarnings("unused")
public class PyKeyedRecordAdapter<K> extends KeyedRecordAdapter<K, Object> {


    /**
     * Create a KeyedRecordAdapter that translates rows of {@code sourceTable} into instances of type {@code T} using
     * the provided {@code recordAdapterDescriptor}, keyed by the given {@code keyColumns}.
     *
     * @param sourceTable The table whose data will be used to create records.
     * @param keyColumns The key columns to use when retrieving data
     */
    public PyKeyedRecordAdapter(@NotNull Table sourceTable, @NotNull String[] keyColumns, String[] dataColumns) {
        super(sourceTable, new RecordAdapterDescriptor<>() {
            @Override
            public Map<String, RecordUpdater<Object, ?>> getColumnAdapters() {
                // must be LinkedHashMap; what's important
                LinkedHashMap<String, RecordUpdater<Object, ?>> map = new LinkedHashMap<>();
                for (String colName : ArrayUtils.addAll(keyColumns, dataColumns)) {
                    final Class<Object> colType = sourceTable.getColumnSource(colName).getType();

                    // no-op record updater -- only returns appropriate type, does not populate anything
                    final RecordUpdater<Object, ?> noOpRecordUpdater =
                            ObjRecordUpdater.getBoxingUpdater(
                                    colType,
                                    ObjRecordUpdater.getObjectUpdater((r, t) -> {
                                        throw new IllegalStateException();
                                    }));

                    if (map.put(colName, noOpRecordUpdater) != null) {
                        throw new IllegalStateException("Duplicate column name: \"" + colName + '"');
                    }
                }
                return map;
            }

            @NotNull
            @Override
            public Object getEmptyRecord() {
                // dummy object just used to get a type while creating MultiRowRecordAdapter
                return new Object();
            }

            @Override
            public BiFunction<Table, RecordAdapterDescriptor<Object>, MultiRowRecordAdapter<Object>> getMultiRowAdapterSupplier() {
                return DefaultMultiRowRecordAdapter::create;
            }

            @Override
            public BiFunction<PartitionedTable, RecordAdapterDescriptor<Object>, MultiRowRecordAdapter<Object>> getMultiRowPartitionedTableAdapterSupplier() {
                return DefaultMultiRowRecordAdapter::create;
            }
        }, keyColumns);
    }

    /**
     * Retrieves data corresponding to the {@code dataKeys} and returns a {@code RecordRetrievalResult} that can be used
     * from Python to create the appropriate result objects.
     *
     * @param dataKeys The keys to retrieve data for.
     * @return The {@code RecordRetrievalResult} with table data corresponding to the rows for the {@code dataKeys}.
     */
    public RecordRetrievalResult getRecordsForPython(final Object[] dataKeys) {
        final List<?> dataKeysList;
        if (isSingleKeyCol) {
            // Each of the dataKeys is an individual key value -- just wrap them in a list.
            dataKeysList = Arrays.asList(dataKeys);
        } else {
            // Each of the dataKeys should be an array -- convert each key array to a list,
            // then wrap those lists in a list.
            dataKeysList = Arrays.stream(dataKeys).map(o -> Arrays.asList((Object[]) o))
                    .collect(Collectors.toUnmodifiableList());
        }
        return getRecordsForPython0(dataKeysList);
    }

    public RecordRetrievalResult getRecordsForPython(final short[] dataKeys) {
        Require.eqTrue(isSingleKeyCol, "isSingleKeyCol");
        return getRecordsForPython0(Arrays.asList(TypeUtils.box(dataKeys)));
    }

    public RecordRetrievalResult getRecordsForPython(final int[] dataKeys) {
        Require.eqTrue(isSingleKeyCol, "isSingleKeyCol");
        return getRecordsForPython0(Arrays.asList(TypeUtils.box(dataKeys)));
    }

    public RecordRetrievalResult getRecordsForPython(final float[] dataKeys) {
        Require.eqTrue(isSingleKeyCol, "isSingleKeyCol");
        return getRecordsForPython0(Arrays.asList(TypeUtils.box(dataKeys)));
    }

    public RecordRetrievalResult getRecordsForPython(final long[] dataKeys) {
        Require.eqTrue(isSingleKeyCol, "isSingleKeyCol");
        return getRecordsForPython0(Arrays.asList(TypeUtils.box(dataKeys)));
    }

    public RecordRetrievalResult getRecordsForPython(final double[] dataKeys) {
        Require.eqTrue(isSingleKeyCol, "isSingleKeyCol");
        return getRecordsForPython0(Arrays.asList(TypeUtils.box(dataKeys)));
    }

    private RecordRetrievalResult getRecordsForPython0(final List<?> dataKeysList) {
        final int nKeys = dataKeysList.size();

        // Convert data keys (object or List<?>) to map keys (object or tuple)
        final List<Object> mapKeys = dataKeysListToLookupKeys.apply(dataKeysList);

        // create arrays to hold the result data
        final MutableObject<Object[]> recordDataArrsRef = new MutableObject<>(null);

        // list to store the index keys for which data is retrieved
        final TLongList recordDataRowKeys = new TLongArrayList(nKeys);

        // map of index keys to the position of the corresponding data key (in the 'dataKeys' list)
        final MutableObject<TLongIntMap> dbRowKeyToDataKeyPositionalIndexRef = new MutableObject<>();

        DO_LOCKED_FUNCTION.accept(
                (usePrev) -> retrieveDataMultipleKeys(mapKeys, recordDataArrsRef, recordDataRowKeys,
                        dbRowKeyToDataKeyPositionalIndexRef, usePrev),
                "KeyedRecordAdapter.getRecords()");

        return new RecordRetrievalResult(recordDataRowKeys.toArray(), dbRowKeyToDataKeyPositionalIndexRef.getValue(),
                recordDataArrsRef.getValue());
    }

    public static class RecordRetrievalResult {

        @NotNull
        public final long[] recordDataRowKeys;

        @NotNull
        public final TLongIntMap rowKeyToDataKeyPositionalIndex;

        /**
         * The arrays of data retrieved from the {@code dataColumns} passed to the constructor.
         */
        @NotNull
        public final Object[] recordDataArrs;

        public RecordRetrievalResult(@NotNull long[] recordDataRowKeys,
                @NotNull TLongIntMap rowKeyToDataKeyPositionalIndex, @NotNull Object[] recordDataArrs) {
            this.recordDataRowKeys = recordDataRowKeys;
            this.rowKeyToDataKeyPositionalIndex = rowKeyToDataKeyPositionalIndex;
            this.recordDataArrs = recordDataArrs;
        }
    }

}
