package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsRangeSet;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.TableData;
import io.deephaven.web.client.api.subscription.SubscriptionTableData.UpdateEventData;
import io.deephaven.web.client.fu.JsSettings;
import io.deephaven.web.shared.data.Range;
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

    private final long[] indexes = new long[0];// in the browser, this array can grow
    private final Map<String, Map<JsFunction<Any, Any>, JsArray<Any>>> cachedData = new HashMap<>();

    public ChartData(JsTable table) {
        this.table = table;
    }

    public void update(Object eventDetail) {
        assert eventDetail instanceof UpdateEventData : "update() can only take table subscription event instances";

        UpdateEventData tableData = (UpdateEventData) eventDetail;
        Iterator<Range> addedIterator = tableData.getAdded().getRange().rangeIterator();
        Iterator<Range> removedIterator = tableData.getRemoved().getRange().rangeIterator();
        Iterator<Range> modifiedIterator = tableData.getModified().getRange().rangeIterator();

        Range nextAdded = addedIterator.hasNext() ? addedIterator.next() : null;
        Range nextRemoved = removedIterator.hasNext() ? removedIterator.next() : null;
        Range nextModified = modifiedIterator.hasNext() ? modifiedIterator.next() : null;
        int i = 0;

        // not useful for adding/modifying data, but fast and easy to use when removing rows
        JsArray<Any>[] allColumns;
        if (nextRemoved != null) {
            // noinspection unchecked
            allColumns = cachedData.values().stream().flatMap(m -> m.values().stream()).toArray(JsArray[]::new);
        } else {
            allColumns = null;
        }

        while (nextAdded != null || nextRemoved != null || nextModified != null) {
            if (i >= indexes.length) {
                // We're past the end, nothing possibly left to remove, just append all the new items
                // Note that this is the first case we'll hit on initial snapshot
                assert nextRemoved == null;
                assert nextModified == null;
                while (nextAdded != null) {
                    insertDataRange(tableData, nextAdded, i);

                    // increment i past these new items so our offset is correct if there is a next block
                    i += nextAdded.size();

                    // not bothering with i or lastIndexSeen since we will break after this while loop
                    nextAdded = addedIterator.hasNext() ? addedIterator.next() : null;
                }
                break;
            }

            long nextIndex = indexes[i];

            // check for added items first, since we will insert items just before the current
            // index, while the other two start at the current index
            if (nextAdded != null && nextAdded.getFirst() < nextIndex) {
                assert nextAdded.getLast() < nextIndex;// the whole range should be there if any is

                // update the index array and insert the actual data into our mapped columns
                insertDataRange(tableData, nextAdded, i);

                // increment i past these new items, so that our "next" is actually next
                i += nextAdded.size();

                nextAdded = addedIterator.hasNext() ? addedIterator.next() : null;
            } else if (nextModified != null && nextModified.getFirst() == nextIndex) {
                assert nextModified.getLast() >= nextIndex; // somehow being asked to update an item which wasn't
                                                            // present

                // the updated block is contiguous, make sure there are at least that many items to tweak
                assert indexes.length - i >= nextModified.size();

                replaceDataRange(tableData, nextModified, i);

                // advance i past this section, since no other change can happen to these rows
                i += nextModified.size();

                nextModified = modifiedIterator.hasNext() ? modifiedIterator.next() : null;
            } else if (nextRemoved != null && nextRemoved.getFirst() == nextIndex) {
                assert nextRemoved.getLast() >= nextIndex; // somehow being asked to remove an item which wasn't present

                // the block being removed is contiguous, so make sure there are at least that many and splice them out
                assert indexes.length - i >= nextRemoved.size();

                // splice out the indexes
                asArray(indexes).splice(i, (int) nextRemoved.size());

                // splice out the data itself
                assert allColumns != null;
                for (JsArray<Any> column : allColumns) {
                    column.splice(i, (int) nextRemoved.size());
                }

                // don't in/decrement i, we'll just keep going
                nextRemoved = removedIterator.hasNext() ? removedIterator.next() : null;
            } else {

                // no match, keep reading
                i++;
            }
        }

        if (JsSettings.isDevMode()) {
            assert ((UpdateEventData) eventDetail).getRows().length == indexes.length;
            assert cachedData.values().stream().flatMap(m -> m.values().stream())
                    .allMatch(arr -> arr.length == indexes.length);
            assert cachedData.values().stream().flatMap(m -> m.values().stream()).allMatch(arr -> arr
                    .reduce((Object val, Any p1, int p2, Any[] p3) -> ((Integer) val) + 1, 0) == indexes.length);

            JsRangeSet fullIndex = tableData.getFullIndex();
            PrimitiveIterator.OfLong iter = fullIndex.getRange().indexIterator();
            for (int j = 0; j < indexes.length; j++) {
                assert indexes[j] == iter.nextLong();
            }
        }
    }

    private void replaceDataRange(UpdateEventData tableData, Range range, int offset) {
        // we don't touch the indexes at all, only need to walk each column and replace values in this range
        for (Entry<String, Map<JsFunction<Any, Any>, JsArray<Any>>> columnMap : cachedData.entrySet()) {
            Column col = table.findColumn(columnMap.getKey());
            for (Entry<JsFunction<Any, Any>, JsArray<Any>> mapFuncAndArray : columnMap.getValue().entrySet()) {
                JsFunction<Any, Any> func = mapFuncAndArray.getKey();
                JsArray<Any> arr = mapFuncAndArray.getValue();

                // rather than getting a slice and splicing it in, just update each value
                if (func == null) {
                    for (int i = 0; i < range.size(); i++) {
                        arr.setAt(offset + i, tableData.getData(range.getFirst() + i, col));
                    }
                } else {
                    for (int i = 0; i < range.size(); i++) {
                        arr.setAt(offset + i, func.apply(tableData.getData(range.getFirst() + i, col)));
                    }
                }
            }
        }
    }

    private void insertDataRange(UpdateEventData tableData, Range range, int offset) {
        // splice in the new indexes
        batchSplice(offset, asArray(indexes), longs(range));

        // splice in data to each column
        for (Entry<String, Map<JsFunction<Any, Any>, JsArray<Any>>> columnMap : cachedData.entrySet()) {
            String columnName = columnMap.getKey();
            Column col = table.findColumn(columnName);
            for (Entry<JsFunction<Any, Any>, JsArray<Any>> mapFuncAndArray : columnMap.getValue().entrySet()) {
                JsFunction<Any, Any> func = mapFuncAndArray.getKey();
                JsArray<Any> arr = mapFuncAndArray.getValue();

                // here we create a new array and splice it in, to avoid slowly growing the data array in the case
                // of many rows being added
                Any[] values = values(tableData, func, col, range);
                batchSplice(offset, arr, values);
            }
        }
    }

    private Any[] batchSplice(int offset, JsArray<Any> existingData, Any[] dataToInsert) {
        int lengthToInsert = dataToInsert.length;
        JsArray<Any> jsArrayToInsert = Js.uncheckedCast(dataToInsert);

        // technically we can do 2^16 args at a time, but we need some wiggle room
        int batchSize = 1 << 15;

        for (int i = 0; i < lengthToInsert; i += batchSize) {
            existingData.splice(offset + i, 0, jsArrayToInsert.slice(i, Math.min(i + batchSize, lengthToInsert)));
        }

        return Js.uncheckedCast(existingData);
    }

    private Any[] values(UpdateEventData tableData, JsFunction<Any, Any> mapFunc, Column col, Range insertedRange) {
        JsArray<Any> result = new JsArray<>();

        if (mapFunc == null) {
            for (int i = 0; i < insertedRange.size(); i++) {
                result.push(tableData.getData(insertedRange.getFirst() + i, col));
            }
        } else {
            for (int i = 0; i < insertedRange.size(); i++) {
                result.push(mapFunc.apply(tableData.getData(insertedRange.getFirst() + i, col)));
            }
        }

        return Js.uncheckedCast(result);

    }

    private static JsArray<Any> asArray(Object obj) {
        return Js.uncheckedCast(obj);
    }

    private static Any[] longs(Range range) {
        long[] longs = new long[(int) range.size()];

        for (int i = 0; i < longs.length; i++) {
            longs[i] = range.getFirst() + i;
        }

        return Js.uncheckedCast(longs);
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
            return Js.uncheckedCast(currentUpdate.getRows().map((p0, p1, p2) -> p0.get(column)));
        }

        return Js.uncheckedCast(currentUpdate.getRows().map((p0, p1, p2) -> mappingFunc.apply(p0.get(column))));
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
