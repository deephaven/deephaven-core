//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.TableData;
import io.deephaven.web.client.api.subscription.AbstractTableSubscription;
import io.deephaven.web.client.api.subscription.SubscriptionTableData;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.util.*;
import java.util.Map.Entry;

/**
 * Helper class to manage snapshots and deltas and keep not only a contiguous JS array of data per column in the
 * underlying table, but also support a mapping function to let client code translate data in some way for display and
 * keep that cached as well.
 */
@JsType(namespace = "dh.plot")
public class ChartData {
    private final JsTable table;

    private RangeSet prevRanges;
    private final Map<String, Map<JsFunction<Any, Any>, JsArray<Any>>> cachedData = new HashMap<>();

    public ChartData(JsTable table) {
        this.table = table;
    }

    public void update(AbstractTableSubscription.SubscriptionEventData tableData) {
        RangeSet positionsForAddedKeys = tableData.getFullIndex().getRange().invert(tableData.getAdded().getRange());
        RangeSet positionsForRemovedKeys = prevRanges.invert(tableData.getRemoved().getRange());
        RangeSet positionsForModifiedKeys =
                tableData.getFullIndex().getRange().invert(tableData.getModified().getRange());
        prevRanges = tableData.getFullIndex().getRange().copy();
        // removes first from the previous set of ranges
        Iterator<Range> removedPositions = positionsForRemovedKeys.reverseRangeIterator();
        JsArray<Any>[] allColumns = null;
        if (removedPositions.hasNext()) {
            // noinspection unchecked
            allColumns = cachedData.values().stream().flatMap(m -> m.values().stream()).toArray(JsArray[]::new);
        }

        while (removedPositions.hasNext()) {
            Range nextRemoved = removedPositions.next();
            for (JsArray<Any> column : allColumns) {
                column.splice((int) nextRemoved.getFirst(), (int) nextRemoved.size());
            }
        }

        // then adds
        Iterator<Range> addedPositions = positionsForAddedKeys.rangeIterator();
        while (addedPositions.hasNext()) {
            Range nextAdded = addedPositions.next();
            insertDataRange(tableData, nextAdded);
        }

        Iterator<Range> modifiedPositions = positionsForModifiedKeys.rangeIterator();
        while (modifiedPositions.hasNext()) {
            Range nextModified = modifiedPositions.next();
            replaceDataRange(tableData, nextModified);
        }
    }

    private void replaceDataRange(SubscriptionTableData tableData, Range positions) {
        RangeSet keys = tableData.getFullIndex().getRange()
                .subsetForPositions(RangeSet.ofRange(positions.getFirst(), positions.getLast()), true);
        // we don't touch the indexes at all, only need to walk each column and replace values in this range
        for (Entry<String, Map<JsFunction<Any, Any>, JsArray<Any>>> columnMap : cachedData.entrySet()) {
            Column col = table.findColumn(columnMap.getKey());
            for (Entry<JsFunction<Any, Any>, JsArray<Any>> mapFuncAndArray : columnMap.getValue().entrySet()) {
                JsFunction<Any, Any> func = mapFuncAndArray.getKey();
                JsArray<Any> arr = mapFuncAndArray.getValue();

                // rather than getting a slice and splicing it in, just update each value
                PrimitiveIterator.OfLong iter = keys.indexIterator();
                int i = 0;
                if (func == null) {
                    while (iter.hasNext()) {
                        arr.setAt(i++, tableData.getData(iter.next(), col));
                    }
                } else {
                    while (iter.hasNext()) {
                        arr.setAt(i++, tableData.getData(iter.next(), col));
                    }
                }
            }
        }
    }

    /**
     * From the event data, insert a contiguous range of rows to each column.
     */
    private void insertDataRange(SubscriptionTableData tableData, Range positions) {
        RangeSet keys = tableData.getFullIndex().getRange()
                .subsetForPositions(RangeSet.ofRange(positions.getFirst(), positions.getLast()), false);
        // splice in data to each column
        for (Entry<String, Map<JsFunction<Any, Any>, JsArray<Any>>> columnMap : cachedData.entrySet()) {
            String columnName = columnMap.getKey();
            Column col = table.findColumn(columnName);
            for (Entry<JsFunction<Any, Any>, JsArray<Any>> mapFuncAndArray : columnMap.getValue().entrySet()) {
                JsFunction<Any, Any> func = mapFuncAndArray.getKey();
                JsArray<Any> arr = mapFuncAndArray.getValue();

                // here we create a new array and splice it in, to avoid slowly growing the data array in the case
                // of many rows being added
                Any[] values = values(tableData, func, col, keys);
                batchSplice((int) positions.getFirst(), arr, values);
            }
        }
    }

    private void batchSplice(int offset, JsArray<Any> existingData, Any[] dataToInsert) {
        int lengthToInsert = dataToInsert.length;
        JsArray<Any> jsArrayToInsert = Js.uncheckedCast(dataToInsert);

        // technically we can do 2^16 args at a time, but we need some wiggle room
        int batchSize = 1 << 15;

        for (int i = 0; i < lengthToInsert; i += batchSize) {
            existingData.splice(offset + i, 0,
                    jsArrayToInsert.slice(i, Math.min(i + batchSize, lengthToInsert)).asArray(new Any[0]));
        }
    }

    private Any[] values(SubscriptionTableData tableData, JsFunction<Any, Any> mapFunc, Column col,
            RangeSet keys) {
        JsArray<Any> result = new JsArray<>();

        PrimitiveIterator.OfLong iter = keys.indexIterator();
        if (mapFunc == null) {
            while (iter.hasNext()) {
                result.push(tableData.getData(iter.next(), col));
            }
        } else {
            while (iter.hasNext()) {
                result.push(mapFunc.apply(tableData.getData(iter.next(), col)));
            }
        }

        return Js.uncheckedCast(result);
    }

    public JsArray<Any> getColumn(String columnName, JsFunction<Any, Any> mappingFunc, TableData currentUpdate) {
        // returns the existing column, if any, otherwise iterates over existing data and builds it

        return cachedData
                .computeIfAbsent(columnName, ignore -> new IdentityHashMap<>())
                .computeIfAbsent(mappingFunc, ignore -> collectAllData(columnName, mappingFunc, currentUpdate));
    }

    /**
     * Helper to build the full array since it hasn't been requested before.
     */
    private JsArray<Any> collectAllData(String columnName, JsFunction<Any, Any> mappingFunc, TableData currentUpdate) {
        Column column = table.findColumn(columnName);
        if (mappingFunc == null) {
            return currentUpdate.getRows().map((p0, p1) -> p0.get(column));
        }

        return currentUpdate.getRows().map((p0, p1) -> mappingFunc.apply(p0.get(column)));
    }

    /**
     * Removes some column from the cache, avoiding extra computation on incoming events, and possibly freeing some
     * memory. If this pair of column name and map function are requested again, it will be recomputed from scratch.
     */
    public void removeColumn(String columnName, JsFunction<Any, Any> mappingFunc) {
        Map<JsFunction<Any, Any>, JsArray<Any>> map = cachedData.get(columnName);
        if (map != null) {
            map.remove(mappingFunc);
        }
    }
}
