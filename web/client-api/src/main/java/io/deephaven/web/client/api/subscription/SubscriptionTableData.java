//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.dom.CustomEventInit;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.fu.JsSettings;
import io.deephaven.web.shared.data.*;
import io.deephaven.web.shared.data.columns.ColumnData;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.NavigableSet;
import java.util.PrimitiveIterator;
import java.util.TreeMap;
import static io.deephaven.web.client.api.subscription.ViewportData.NO_ROW_FORMAT_COLUMN;

public class SubscriptionTableData {
    @JsFunction
    private interface ArrayCopy {
        @SuppressWarnings("unusable-by-js")
        void copyTo(Object destArray, long destPos, Object srcArray, int srcPos);
    }

    private final JsArray<Column> columns;
    private final int rowStyleColumn;
    private final HasEventHandling evented;

    // the actual rows present on the client, in their correct order
    private RangeSet index;

    // mappings from the index to the position of a row in the data array
    private TreeMap<Long, Long> redirectedIndexes;

    // rows in the data columns that no longer contain data and can be reused
    private RangeSet reusableDestinations;

    // array of data columns, cast each to a jsarray to read rows
    private Object[] data;

    public SubscriptionTableData(JsArray<Column> columns, int rowStyleColumn, HasEventHandling evented) {
        this.columns = columns;
        this.rowStyleColumn = rowStyleColumn;
        this.evented = evented;
    }

    // TODO support this being called multiple times so we can keep viewports going without clearing the data
    public TableData handleSnapshot(TableSnapshot snapshot) {
        // when changing snapshots we should actually rewrite the columns, possibly emulate ViewportData more?
        ColumnData[] dataColumns = snapshot.getDataColumns();
        data = new Object[dataColumns.length];
        reusableDestinations = RangeSet.empty();
        redirectedIndexes = new TreeMap<>();
        index = snapshot.getIncludedRows();

        long includedRowCount = snapshot.getIncludedRows().size();
        RangeSet destination = freeRows(includedRowCount);
        boolean indexUpdated = false;

        for (int index = 0; index < dataColumns.length; index++) {
            ColumnData dataColumn = dataColumns[index];
            if (dataColumn == null) {
                // no data in this column, wasn't requested
                continue;
            }

            final int i = index;
            Column column = columns.find((c, i1, i2) -> c.getIndex() == i);

            ArrayCopy arrayCopy = arrayCopyFuncForColumn(column);

            Object[] localCopy = new Object[(int) includedRowCount];
            data[index] = localCopy;
            PrimitiveIterator.OfLong destIter = destination.indexIterator();
            PrimitiveIterator.OfLong indexIter = snapshot.getIncludedRows().indexIterator();
            int j = 0;
            while (indexIter.hasNext()) {
                assert destIter.hasNext();
                long dest = destIter.nextLong();
                long nextIndex = indexIter.nextLong();
                if (indexUpdated) {
                    redirectedIndexes.put(nextIndex, dest);
                }
                arrayCopy.copyTo(localCopy, dest, dataColumn.getData(), j++);
            }
            assert !destIter.hasNext();
            indexUpdated = true;
        }

        return notifyUpdates(index, RangeSet.empty(), RangeSet.empty());
    }

    /**
     * Helper to avoid appending many times when modifying indexes. The append() method should be called for each key
     * _in order_ to ensure that RangeSet.addRange isn't called excessively. When no more items will be added, flush()
     * must be called.
     */
    private static class RangeSetAppendHelper {
        private final RangeSet rangeSet;

        private long currentFirst = -1;
        private long currentLast;

        public RangeSetAppendHelper(final RangeSet rangeSet) {
            this.rangeSet = rangeSet;
        }

        public void append(long key) {
            assert key >= 0;

            if (currentFirst == -1) {
                // first key to be added, move both first and last
                currentFirst = key;
                currentLast = key;

                return;
            }

            if (key == currentLast + 1) {
                // key appends to our current range
                currentLast = key;
            } else if (key == currentFirst - 1) {
                // key appends to our current range
                currentFirst = key;
            } else {
                // existing range doesn't match the new item, finish the old range and start a new one
                rangeSet.addRange(new Range(currentFirst, currentLast));
                currentFirst = key;
                currentLast = key;
            }
        }

        public void flush() {
            if (currentFirst != -1) {
                rangeSet.addRange(new Range(currentFirst, currentLast));
                currentFirst = -1;
            }
        }
    }

    public TableData handleDelta(DeltaUpdates delta) {
        // delete old data, track slots freed up. we do this by row since they might be non-contiguous or out of order
        RangeSetAppendHelper reusableHelper = new RangeSetAppendHelper(reusableDestinations);
        delta.getRemoved().indexIterator().forEachRemaining((long index) -> {
            long dest = redirectedIndexes.remove(index);
            reusableHelper.append(dest);
            // TODO consider trimming the columns down too, and truncating the reusable slots at the end
        });
        reusableHelper.flush();
        // clean up index by ranges, not by row
        delta.getRemoved().rangeIterator().forEachRemaining(index::removeRange);

        // Shift moved rows in the redir index
        boolean hasReverseShift = false;
        final ShiftedRange[] shiftedRanges = delta.getShiftedRanges();
        RangeSetAppendHelper shifter = new RangeSetAppendHelper(index);
        for (int i = shiftedRanges.length - 1; i >= 0; --i) {
            final ShiftedRange shiftedRange = shiftedRanges[i];
            final long offset = shiftedRange.getDelta();
            if (offset < 0) {
                hasReverseShift = true;
                continue;
            }
            index.removeRange(shiftedRange.getRange());
            final NavigableSet<Long> toMove = redirectedIndexes.navigableKeySet()
                    .subSet(shiftedRange.getRange().getFirst(), true, shiftedRange.getRange().getLast(), true);
            // iterate backward and move them forward
            for (Long key : toMove.descendingSet()) {
                long shiftedKey = key + offset;
                Long oldValue = redirectedIndexes.put(shiftedKey, redirectedIndexes.remove(key));
                assert oldValue == null : shiftedKey + " already has a value, " + oldValue;
                shifter.append(shiftedKey);
            }
        }
        if (hasReverseShift) {
            for (int i = 0; i < shiftedRanges.length; ++i) {
                final ShiftedRange shiftedRange = shiftedRanges[i];
                final long offset = shiftedRange.getDelta();
                if (offset > 0) {
                    continue;
                }
                index.removeRange(shiftedRange.getRange());
                final NavigableSet<Long> toMove = redirectedIndexes.navigableKeySet()
                        .subSet(shiftedRange.getRange().getFirst(), true, shiftedRange.getRange().getLast(), true);
                // iterate forward and move them backward
                for (Long key : toMove) {
                    long shiftedKey = key + offset;
                    Long oldValue = redirectedIndexes.put(shiftedKey, redirectedIndexes.remove(key));
                    assert oldValue == null : shiftedKey + " already has a value, " + oldValue;
                    shifter.append(shiftedKey);
                }
            }
        }
        shifter.flush();

        // Find space for the rows we're about to add. We must not adjust the index until this is done, it is used
        // to see where the end of the data is
        RangeSet addedDestination = freeRows(delta.getAdded().size());
        // Within each column, append additions
        DeltaUpdates.ColumnAdditions[] additions = delta.getSerializedAdditions();
        for (int i = 0; i < additions.length; i++) {
            DeltaUpdates.ColumnAdditions addedColumn = delta.getSerializedAdditions()[i];
            Column column = columns.find((c, i1, i2) -> c.getIndex() == addedColumn.getColumnIndex());

            ArrayCopy arrayCopy = arrayCopyFuncForColumn(column);

            PrimitiveIterator.OfLong addedIndexes = delta.getAdded().indexIterator();
            PrimitiveIterator.OfLong destIter = addedDestination.indexIterator();
            int j = 0;
            while (addedIndexes.hasNext()) {
                long origIndex = addedIndexes.nextLong();
                assert delta.getIncludedAdditions().contains(origIndex);
                assert destIter.hasNext();
                long dest = destIter.nextLong();
                Long old = redirectedIndexes.put(origIndex, dest);
                assert old == null || old == dest;
                arrayCopy.copyTo(data[addedColumn.getColumnIndex()], dest, addedColumn.getValues().getData(), j++);
            }
        }

        // Update the index to reflect the added items
        delta.getAdded().rangeIterator().forEachRemaining(index::addRange);

        // Within each column, apply modifications
        DeltaUpdates.ColumnModifications[] modifications = delta.getSerializedModifications();
        RangeSet allModified = new RangeSet();
        for (int i = 0; i < modifications.length; ++i) {
            final DeltaUpdates.ColumnModifications modifiedColumn = modifications[i];
            if (modifiedColumn == null) {
                continue;
            }

            modifiedColumn.getRowsIncluded().rangeIterator().forEachRemaining(allModified::addRange);
            Column column = columns.find((c, i1, i2) -> c.getIndex() == modifiedColumn.getColumnIndex());

            ArrayCopy arrayCopy = arrayCopyFuncForColumn(column);

            PrimitiveIterator.OfLong modifiedIndexes = modifiedColumn.getRowsIncluded().indexIterator();
            int j = 0;
            while (modifiedIndexes.hasNext()) {
                long origIndex = modifiedIndexes.nextLong();
                arrayCopy.copyTo(data[modifiedColumn.getColumnIndex()], redirectedIndexes.get(origIndex),
                        modifiedColumn.getValues().getData(), j++);
            }
        }

        // Check that the index sizes make sense
        assert redirectedIndexes.size() == index.size();
        // Note that we can't do this assert, since we don't truncate arrays, we just leave nulls at the end
        // assert Js.asArrayLike(data[0]).getLength() == redirectedIndexes.size();

        return notifyUpdates(delta.getAdded(), delta.getRemoved(), allModified);
    }

    private TableData notifyUpdates(RangeSet added, RangeSet removed, RangeSet modified) {
        UpdateEventData detail = new UpdateEventData(added, removed, modified);
        if (evented != null) {
            CustomEventInit event = CustomEventInit.create();
            event.setDetail(detail);
            evented.fireEvent(TableSubscription.EVENT_UPDATED, event);
        }
        return detail;
    }

    private ArrayCopy arrayCopyFuncForColumn(@Nullable Column column) {
        final String type = column != null ? column.getType() : "";
        switch (type) {
            case "long":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final long value = Js.asArrayLike(srcArray).getAtAsAny(srcPos).asLong();
                    if (value == QueryConstants.NULL_LONG) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, LongWrapper.of(value));
                    }
                };
            case "java.time.Instant":
            case "java.time.ZonedDateTime":
                return (destArray, destPos, srcArray, srcPos) -> {
                    long value = Js.asArrayLike(srcArray).getAtAsAny(srcPos).asLong();
                    if (value == QueryConstants.NULL_LONG) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, new DateWrapper(value));
                    }
                };
            case "java.math.BigDecimal":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final BigDecimal value = Js.cast(Js.asArrayLike(srcArray).getAt(srcPos));
                    if (value == null) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, new BigDecimalWrapper(value));
                    }
                };
            case "java.math.BigInteger":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final BigInteger value = Js.cast(Js.asArrayLike(srcArray).getAt(srcPos));
                    if (value == null) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, new BigIntegerWrapper(value));
                    }
                };
            case "java.time.LocalDate":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final LocalDate value = Js.cast(Js.asArrayLike(srcArray).getAt(srcPos));
                    if (value == null) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, new LocalDateWrapper(value));
                    }
                };
            case "java.time.LocalTime":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final LocalTime value = Js.cast(Js.asArrayLike(srcArray).getAt(srcPos));
                    if (value == null) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, new LocalTimeWrapper(value));
                    }
                };
            case "java.lang.Boolean":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final Any value = Js.asArrayLike(srcArray).getAtAsAny(srcPos);

                    if (value == null) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else if (value.asBoolean()) {
                        Js.asArrayLike(destArray).setAt((int) destPos, true);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, false);
                    }
                };
            case "int":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final int value = Js.asArrayLike(srcArray).getAtAsAny(srcPos).asInt();
                    if (value == QueryConstants.NULL_INT) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, value);
                    }
                };
            case "byte":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final byte value = Js.asArrayLike(srcArray).getAtAsAny(srcPos).asByte();
                    if (value == QueryConstants.NULL_BYTE) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, value);
                    }
                };
            case "short":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final short value = Js.asArrayLike(srcArray).getAtAsAny(srcPos).asShort();
                    if (value == QueryConstants.NULL_SHORT) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, value);
                    }
                };
            case "double":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final double value = Js.asArrayLike(srcArray).getAtAsAny(srcPos).asDouble();
                    if (value == QueryConstants.NULL_DOUBLE) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, value);
                    }
                };
            case "float":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final float value = Js.asArrayLike(srcArray).getAtAsAny(srcPos).asFloat();
                    if (value == QueryConstants.NULL_FLOAT) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, value);
                    }
                };
            case "char":
                return (destArray, destPos, srcArray, srcPos) -> {
                    final char value = Js.asArrayLike(srcArray).getAtAsAny(srcPos).asChar();
                    if (value == QueryConstants.NULL_CHAR) {
                        Js.asArrayLike(destArray).setAt((int) destPos, null);
                    } else {
                        Js.asArrayLike(destArray).setAt((int) destPos, value);
                    }
                };
            default:
                // exit so we can handle null also in the method's final return
        }
        return (destArray, destPos, srcArray, srcPos) -> {
            // boring column or format data, just copy it
            Js.asArrayLike(destArray).setAt((int) destPos, Js.asArrayLike(srcArray).getAt(srcPos));
        };
    }

    private RangeSet freeRows(long required) {
        if (required == 0) {
            return RangeSet.empty();
        }
        long existingSlotsToReuse = reusableDestinations.size();
        if (existingSlotsToReuse > required) {
            // only take some of the ranges from the reusable list
            RangeSet reused = RangeSet.empty();
            long taken = 0;
            RangeSet stillUnused = RangeSet.empty();
            // TODO this could be more efficient, iterating entire ranges until we only need a partial range
            PrimitiveIterator.OfLong iterator = reusableDestinations.indexIterator();
            while (taken < required) {
                assert iterator.hasNext();
                long value = iterator.nextLong();
                reused.addRange(new Range(value, value));
                taken++;
            }
            assert taken == required;
            while (iterator.hasNext()) {
                long value = iterator.nextLong();
                stillUnused.addRange(new Range(value, value));
            }
            reusableDestinations = stillUnused;
            assert required == reused.size();
            return reused;
        }
        // take all ranges from the reusable list (plus make more if needed)
        RangeSet created = reusableDestinations;
        if (existingSlotsToReuse < required) {
            long nextIndex;
            if (created.isEmpty()) {
                if (index.isEmpty()) {
                    nextIndex = 0;
                } else {
                    nextIndex = redirectedIndexes.size();
                }
            } else if (index.isEmpty()) {
                nextIndex = created.getLastRow() + 1;
            } else {
                nextIndex = Math.max(created.getLastRow(), index.getLastRow()) + 1;
            }
            created.addRange(new Range(nextIndex, nextIndex + required - existingSlotsToReuse - 1));
        }

        reusableDestinations = RangeSet.empty();
        assert required == created.size();
        return created;
    }

    @TsInterface
    @TsName(namespace = "dh")
    public class SubscriptionRow implements TableData.Row {
        private final long index;
        public LongWrapper indexCached;

        public SubscriptionRow(long index) {
            this.index = index;
        }

        @Override
        public LongWrapper getIndex() {
            if (indexCached == null) {
                indexCached = LongWrapper.of(index);
            }
            return indexCached;
        }

        @Override
        public Any get(Column column) {
            int redirectedIndex = (int) (long) redirectedIndexes.get(this.index);
            JsArrayLike<Object> columnData = Js.asArrayLike(data[column.getIndex()]);
            return columnData.getAtAsAny(redirectedIndex);
        }

        @Override
        public Format getFormat(Column column) {
            long cellColors = 0;
            long rowColors = 0;
            String numberFormat = null;
            String formatString = null;
            int redirectedIndex = (int) (long) redirectedIndexes.get(this.index);
            if (column.getStyleColumnIndex() != null) {
                JsArray<Any> colors = Js.uncheckedCast(data[column.getStyleColumnIndex()]);
                cellColors = colors.getAtAsAny(redirectedIndex).asLong();
            }
            if (rowStyleColumn != NO_ROW_FORMAT_COLUMN) {
                JsArray<Any> rowStyle = Js.uncheckedCast(data[rowStyleColumn]);
                rowColors = rowStyle.getAtAsAny(redirectedIndex).asLong();
            }
            if (column.getFormatColumnIndex() != null) {
                JsArray<Any> formatStrings = Js.uncheckedCast(data[column.getFormatColumnIndex()]);
                numberFormat = formatStrings.getAtAsAny(redirectedIndex).asString();
            }
            if (column.getFormatStringColumnIndex() != null) {
                JsArray<Any> formatStrings = Js.uncheckedCast(data[column.getFormatStringColumnIndex()]);
                formatString = formatStrings.getAtAsAny(redirectedIndex).asString();
            }
            return new Format(cellColors, rowColors, numberFormat, formatString);
        }
    }


    /**
     * Event data, describing the indexes that were added/removed/updated, and providing access to Rows (and thus data
     * in columns) either by index, or scanning the complete present index.
     *
     * This class supports two ways of reading the table - checking the changes made since the last update, and reading
     * all data currently in the table. While it is more expensive to always iterate over every single row in the table,
     * it may in some cases actually be cheaper than maintaining state separately and updating only the changes, though
     * both options should be considered.
     *
     * The RangeSet objects allow iterating over the LongWrapper indexes in the table. Note that these "indexes" are not
     * necessarily contiguous and may be negative, and represent some internal state on the server, allowing it to keep
     * track of data efficiently. Those LongWrapper objects can be passed to the various methods on this instance to
     * read specific rows or cells out of the table.
     */
    @TsInterface
    @TsName(name = "SubscriptionTableData", namespace = "dh")
    public class UpdateEventData implements TableData {
        private JsRangeSet added;
        private JsRangeSet removed;
        private JsRangeSet modified;

        // cached copy in case it was requested, could be requested again
        private JsArray<SubscriptionRow> allRows;

        public UpdateEventData(RangeSet added, RangeSet removed, RangeSet modified) {
            this.added = new JsRangeSet(added);
            this.removed = new JsRangeSet(removed);
            this.modified = new JsRangeSet(modified);
        }

        /**
         * A lazily computed array of all rows in the entire table
         * 
         * @return {@link SubscriptionRow} array.
         */
        @Override
        public JsArray<SubscriptionRow> getRows() {
            if (allRows == null) {
                allRows = new JsArray<>();
                index.indexIterator().forEachRemaining((long index) -> {
                    allRows.push(new SubscriptionRow(index));
                });
                if (JsSettings.isDevMode()) {
                    assert allRows.length == index.size();
                }
            }
            return allRows;
        }

        @Override
        public Row get(int index) {
            return this.get((long) index);
        }

        /**
         * Reads a row object from the table, from which any subscribed column can be read
         * 
         * @param index
         * @return {@link SubscriptionRow}
         */
        @Override
        public SubscriptionRow get(long index) {
            return new SubscriptionRow(index);
        }

        @Override
        public Any getData(int index, Column column) {
            return getData((long) index, column);
        }

        /**
         * a specific cell from the table, from the specified row and column
         * 
         * @param index
         * @param column
         * @return Any
         */
        @Override
        public Any getData(long index, Column column) {
            int redirectedIndex = (int) (long) redirectedIndexes.get(index);
            JsArrayLike<Object> columnData = Js.asArrayLike(data[column.getIndex()]);
            return columnData.getAtAsAny(redirectedIndex);
        }

        /**
         * the Format to use for a cell from the specified row and column
         * 
         * @param index
         * @param column
         * @return {@link Format}
         */
        @Override
        public Format getFormat(int index, Column column) {
            return getFormat((long) index, column);
        }

        @Override
        public Format getFormat(long index, Column column) {
            long cellColors = 0;
            long rowColors = 0;
            String numberFormat = null;
            String formatString = null;
            int redirectedIndex = (int) (long) redirectedIndexes.get(index);
            if (column.getStyleColumnIndex() != null) {
                JsArray<Any> colors = Js.uncheckedCast(data[column.getStyleColumnIndex()]);
                cellColors = colors.getAtAsAny(redirectedIndex).asLong();
            }
            if (rowStyleColumn != NO_ROW_FORMAT_COLUMN) {
                JsArray<Any> rowStyle = Js.uncheckedCast(data[rowStyleColumn]);
                rowColors = rowStyle.getAtAsAny(redirectedIndex).asLong();
            }
            if (column.getFormatColumnIndex() != null) {
                JsArray<Any> formatStrings = Js.uncheckedCast(data[column.getFormatColumnIndex()]);
                numberFormat = formatStrings.getAtAsAny(redirectedIndex).asString();
            }
            if (column.getFormatStringColumnIndex() != null) {
                JsArray<Any> formatStrings = Js.uncheckedCast(data[column.getFormatStringColumnIndex()]);
                formatString = formatStrings.getAtAsAny(redirectedIndex).asString();
            }
            return new Format(cellColors, rowColors, numberFormat, formatString);
        }

        @Override
        public JsArray<Column> getColumns() {
            return columns;
        }

        /**
         * The ordered set of row indexes added since the last update
         * 
         * @return dh.RangeSet
         */
        @JsProperty
        public JsRangeSet getAdded() {
            return added;
        }

        /**
         * The ordered set of row indexes removed since the last update
         * 
         * @return dh.RangeSet
         */
        @JsProperty
        public JsRangeSet getRemoved() {
            return removed;
        }

        /**
         * The ordered set of row indexes updated since the last update
         * 
         * @return dh.RangeSet
         */
        @JsProperty
        public JsRangeSet getModified() {
            return modified;
        }

        @JsProperty
        public JsRangeSet getFullIndex() {
            return new JsRangeSet(index);
        }
    }
}
