package io.deephaven.queryutil.dataadapter;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongIntMap;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.queryutil.dataadapter.rec.RecordUpdater;
import io.deephaven.queryutil.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by rbasralian on 9/12/22
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

                    // no-op record updater -- only returns appropriate type, does not implement other methods
                    final RecordUpdater<Object, Object> noOpRecordUpdater = new RecordUpdater<>() {
                        @Override
                        public Class<Object> getSourceType() {
                            return colType;
                        }
                    };

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
        final List<Object> mapKeys = dataKeysListToMapKeys.apply(dataKeysList);

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

        return new RecordRetrievalResult(recordDataRowKeys.toArray(), dbRowKeyToDataKeyPositionalIndexRef.getValue(),
                recordDataArrs);
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
