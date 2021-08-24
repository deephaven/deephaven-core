package io.deephaven.web.client.api.subscription;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.web.client.api.*;
import io.deephaven.web.shared.data.*;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PrimitiveIterator.OfLong;
import java.util.Set;

public class ViewportData implements TableData {
    private static final Any NULL_SENTINEL = Js.asAny(new JsObject());

    /**
     * Clean the data at the provided index
     */
    @JsFunction
    private interface DataCleaner {
        void clean(JsArray<Any> data, int index);
    }

    public static final int NO_ROW_FORMAT_COLUMN = -1;

    public class MergeResults {
        public Set<Integer> added = new HashSet<>();
        public Set<Integer> modified = new HashSet<>();
        public Set<Integer> removed = new HashSet<>();
    }

    private long offset;
    private int length;
    private final int maxLength;
    private JsArray<ViewportRow> rows;
    private final JsArray<Column> columns;

    private final Object[] data;

    private final int rowFormatColumn;

    public ViewportData(RangeSet includedRows, Object[] dataColumns, JsArray<Column> columns,
        int rowFormatColumn, long maxLength) {
        assert maxLength <= Integer.MAX_VALUE;
        this.maxLength = (int) maxLength;

        Iterator<Range> rangeIterator = includedRows.rangeIterator();
        data = new Object[dataColumns.length];
        if (rangeIterator.hasNext()) {
            Range range = rangeIterator.next();
            assert !rangeIterator.hasNext() : "Snapshot only supports one range";

            offset = range.getFirst();
            length = (int) (range.getLast() - range.getFirst() + 1);
            assert length == range.size();
        } else {
            offset = -1;
        }
        for (int i = 0; i < columns.length; i++) {
            Column c = columns.getAt(i);
            int index = c.getIndex();
            if (dataColumns[index] == null) {
                // no data for this column, not requested in viewport
                continue;
            }
            data[index] = cleanData(dataColumns[index], c);
            if (c.getStyleColumnIndex() != null) {
                data[c.getStyleColumnIndex()] = dataColumns[c.getStyleColumnIndex()];
            }
            if (c.getFormatStringColumnIndex() != null) {
                data[c.getFormatStringColumnIndex()] = dataColumns[c.getFormatStringColumnIndex()];
            }
        }
        if (length < maxLength) {
            for (int i = 0; i < data.length; i++) {
                if (data[i] != null) {
                    JsArray<Any> existingColumnData = Js.uncheckedCast(data[i]);
                    existingColumnData.length = this.maxLength;
                    existingColumnData.fill(NULL_SENTINEL, length, this.maxLength);
                }
            }
        }
        this.rowFormatColumn = rowFormatColumn;
        if (rowFormatColumn != NO_ROW_FORMAT_COLUMN) {
            data[rowFormatColumn] = dataColumns[rowFormatColumn];
        }

        rows = new JsArray<>();
        for (int i = 0; i < length; i++) {
            rows.push(new ViewportRow(i, data, data[rowFormatColumn]));
        }
        this.columns = JsObject.freeze(Js.uncheckedCast(columns.slice()));
    }

    private static DataCleaner getDataCleanerForColumnType(String columnType) {
        switch (columnType) {
            case "int":
                return (data, i) -> {
                    int value = data.getAnyAt(i).asInt();
                    if (value == QueryConstants.NULL_INT) {
                        data.setAt(i, null);
                    }
                };
            case "byte":
                return (data, i) -> {
                    byte value = data.getAnyAt(i).asByte();
                    if (value == QueryConstants.NULL_BYTE) {
                        data.setAt(i, null);
                    }
                };
            case "short":
                return (data, i) -> {
                    short value = data.getAnyAt(i).asShort();
                    if (value == QueryConstants.NULL_SHORT) {
                        data.setAt(i, null);
                    }
                };
            case "double":
                return (data, i) -> {
                    double value = data.getAnyAt(i).asDouble();
                    if (value == QueryConstants.NULL_DOUBLE) {
                        data.setAt(i, null);
                    }
                };
            case "float":
                return (data, i) -> {
                    float value = data.getAnyAt(i).asFloat();
                    if (value == QueryConstants.NULL_FLOAT) {
                        data.setAt(i, null);
                    }
                };
            case "char":
                return (data, i) -> {
                    char value = data.getAnyAt(i).asChar();
                    if (value == QueryConstants.NULL_CHAR) {
                        data.setAt(i, null);
                    }
                };
            default:
                return null;
        }
    }

    public static Object cleanData(Object dataColumn, Column column) {
        if (dataColumn == null) {
            return null;
        }
        if (column == null) {
            return dataColumn;
        }

        switch (column.getType()) {
            case "long": {
                JsArray<Any> values = Js.uncheckedCast(dataColumn);
                LongWrapper[] cleanData = new LongWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    long value = values.getAnyAt(i).asLong();
                    if (value == QueryConstants.NULL_LONG) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = LongWrapper.of(value);
                    }
                }
                return cleanData;
            }
            case "io.deephaven.db.tables.utils.DBDateTime": {
                JsArray<Any> values = Js.uncheckedCast(dataColumn);
                DateWrapper[] cleanData = new DateWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    long value = values.getAnyAt(i).asLong();
                    if (value == QueryConstants.NULL_LONG) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new DateWrapper(value);
                    }
                }
                return cleanData;
            }
            case "java.lang.Boolean": {
                JsArray<Any> values = Js.uncheckedCast(dataColumn);
                java.lang.Boolean[] cleanData = new java.lang.Boolean[values.length];
                for (int i = 0; i < values.length; i++) {
                    int value = values.getAnyAt(i).asInt();
                    if (value == 1) {
                        cleanData[i] = true;
                    } else if (value == 0) {
                        cleanData[i] = false;
                    } else {
                        cleanData[i] = null;
                    }
                }
                return cleanData;
            }
            case "java.math.BigDecimal": {
                final JsArray<Any> values = Js.uncheckedCast(dataColumn);
                final BigDecimalWrapper[] cleanData = new BigDecimalWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    final BigDecimal value = Js.cast(values.getAt(i));
                    if (value == null) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new BigDecimalWrapper(value);
                    }
                }
                return cleanData;
            }
            case "java.math.BigInteger": {
                final JsArray<Any> values = Js.uncheckedCast(dataColumn);
                final BigIntegerWrapper[] cleanData = new BigIntegerWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    final BigInteger value = Js.cast(values.getAt(i));
                    if (value == null) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new BigIntegerWrapper(value);
                    }
                }
                return cleanData;
            }
            case "java.time.LocalDate": {
                final JsArray<Any> values = Js.uncheckedCast(dataColumn);
                final LocalDateWrapper[] cleanData = new LocalDateWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    final LocalDate value = Js.cast(values.getAt(i));
                    if (value == null) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new LocalDateWrapper(value);
                    }
                }
                return cleanData;
            }
            case "java.time.LocalTime": {
                final JsArray<Any> values = Js.uncheckedCast(dataColumn);
                final LocalTimeWrapper[] cleanData = new LocalTimeWrapper[values.length];
                for (int i = 0; i < values.length; i++) {
                    final LocalTime value = Js.cast(values.getAt(i));
                    if (value == null) {
                        cleanData[i] = null;
                    } else {
                        cleanData[i] = new LocalTimeWrapper(value);
                    }
                }
                return cleanData;
            }
            default:
                DataCleaner dataCleaner = getDataCleanerForColumnType(column.getType());
                if (dataCleaner != null) {
                    JsArray<Any> values = Js.uncheckedCast(dataColumn);
                    JsArray<Any> cleanData = Js.uncheckedCast(values.slice());

                    for (int i = 0; i < values.length; i++) {
                        dataCleaner.clean(cleanData, i);
                    }

                    return cleanData;
                } else {
                    return dataColumn;
                }
        }
    }

    @JsProperty
    public double getOffset() {
        return offset;

    }

    @Override
    public Row get(long index) {
        return getRows().getAt((int) index);
    }

    @Override
    public Row get(int index) {
        return getRows().getAt(index);
    }

    @Override
    public Any getData(int index, Column column) {
        return getRows().getAt(index).get(column);
    }

    @Override
    public Any getData(long index, Column column) {
        return getRows().getAt((int) index).get(column);
    }

    @Override
    public Format getFormat(int index, Column column) {
        return getRows().getAt(index).getFormat(column);
    }

    @Override
    public Format getFormat(long index, Column column) {
        return getRows().getAt((int) index).getFormat(column);
    }

    @Override
    @JsProperty
    public JsArray<ViewportRow> getRows() {
        if (rows.length != length) {
            rows = new JsArray<>();
            for (int i = 0; i < length; i++) {
                rows.push(new ViewportRow(i, data, data[rowFormatColumn]));
            }
            JsObject.freeze(rows);
        }
        return rows;
    }

    @Override
    @JsProperty
    public JsArray<Column> getColumns() {
        return columns;
    }

    public MergeResults merge(DeltaUpdates updates) {
        if (offset == -1 && updates.getIncludedAdditions().size() > 0) {
            offset = updates.getIncludedAdditions().getFirstRow();
        }
        final MergeResults updated = new MergeResults();

        // First we remove rows by nulling them out.
        updates.getRemoved().indexIterator().forEachRemaining((long removedIndex) -> {
            int internalOffset = (int) (removedIndex - offset);
            if (internalOffset < 0 || internalOffset >= length) {
                return;
            }
            for (int i = 0; i < data.length; i++) {
                JsArray<Any> existingColumnData = Js.uncheckedCast(data[i]);
                if (existingColumnData == null) {
                    continue;
                }
                existingColumnData.setAt(internalOffset, NULL_SENTINEL);
            }
            updated.removed.add(internalOffset);
        });

        // Now we shift data around.
        boolean hasReverseShift = false;
        final ShiftedRange[] shiftedRanges = updates.getShiftedRanges();

        // must apply shifts in mem-move semantics; so we shift forward from right to left first
        for (int si = shiftedRanges.length - 1; si >= 0; --si) {
            final ShiftedRange shiftedRange = shiftedRanges[si];
            final long shiftDelta = shiftedRange.getDelta();
            if (shiftDelta < 0) {
                hasReverseShift = true;
                continue;
            }

            final long beginAsLong = Math.max(shiftedRange.getRange().getFirst() - offset, 0);
            final int end = (int) Math.min(shiftedRange.getRange().getLast() - offset, length - 1);
            if (end < beginAsLong) {
                // this range is out of our viewport
                continue;
            }

            // long math is expensive; so convert to int early/once
            final int begin = (int) beginAsLong;

            // iterate backward and move them forward
            for (int j = end; j >= begin; --j) {
                for (int i = 0; i < data.length; ++i) {
                    final JsArray<Any> existingColumnData = Js.uncheckedCast(data[i]);
                    if (existingColumnData == null) {
                        continue;
                    }

                    final long internalOffsetAsLong = (j + shiftDelta);
                    if (internalOffsetAsLong >= 0 && internalOffsetAsLong < maxLength) {
                        // because internalOffsetAsLong is less than maxLen; we know it must be fit
                        // in an int
                        final int internalOffset = (int) internalOffsetAsLong;
                        updated.added.add(internalOffset);
                        Any toMove = existingColumnData.getAt(j);
                        existingColumnData.setAt(internalOffset, toMove);
                    }

                    updated.removed.add(j);
                    existingColumnData.setAt(j, NULL_SENTINEL);
                }
            }
        }
        if (hasReverseShift) {
            // then we shift in reverse from left to right
            for (int si = 0; si < shiftedRanges.length; ++si) {
                final ShiftedRange shiftedRange = shiftedRanges[si];
                final long shiftDelta = shiftedRange.getDelta();
                if (shiftDelta > 0) {
                    continue;
                }

                final long begin = Math.max(shiftedRange.getRange().getFirst() - offset, 0);
                final int end =
                    (int) Math.min(shiftedRange.getRange().getLast() - offset, length - 1);
                if (end < begin) {
                    // this range is out of our viewport
                    continue;
                }

                // iterate forward and move them backward (note: since begin is <= end, we now know
                // it fits in an int)
                for (int j = (int) begin; j <= end; ++j) {
                    for (int i = 0; i < data.length; ++i) {
                        final JsArray<Any> existingColumnData = Js.uncheckedCast(data[i]);
                        if (existingColumnData == null) {
                            continue;
                        }

                        final long internalOffsetAsLong = j + shiftDelta;
                        if (internalOffsetAsLong >= 0 && internalOffsetAsLong < maxLength) {
                            // because internalOffsetAsLong is less than maxLen; we know it must be
                            // fit in an int
                            final int internalOffset = (int) internalOffsetAsLong;
                            updated.added.add(internalOffset);
                            existingColumnData.setAt(internalOffset, existingColumnData.getAt(j));
                        }

                        updated.removed.add(j);
                        existingColumnData.setAt(j, NULL_SENTINEL);
                    }
                }
            }
        }

        DeltaUpdates.ColumnModifications[] serializedModifications =
            updates.getSerializedModifications();
        for (int modifiedColIndex =
            0; modifiedColIndex < serializedModifications.length; modifiedColIndex++) {
            final DeltaUpdates.ColumnModifications modifiedColumn =
                serializedModifications[modifiedColIndex];
            final OfLong it =
                modifiedColumn == null ? null : modifiedColumn.getRowsIncluded().indexIterator();

            if (it == null || !it.hasNext()) {
                continue;
            }

            // look for a local Column which matches this index so we know how to clean it
            final Column column =
                columns.find((c, i1, i2) -> c.getIndex() == modifiedColumn.getColumnIndex());
            final JsArray<Any> updatedColumnData =
                Js.uncheckedCast(cleanData(modifiedColumn.getValues().getData(), column));
            final JsArray<Any> existingColumnData =
                Js.uncheckedCast(data[modifiedColumn.getColumnIndex()]);
            if (updatedColumnData.length == 0) {
                continue;
            }

            // for each change provided for this column, replace the values in our store
            int i = 0;
            while (it.hasNext()) {
                long modifiedOffset = it.nextLong();
                long internalOffset = (modifiedOffset - offset);
                if (internalOffset < 0 || internalOffset >= maxLength) {
                    i++;
                    continue;// data we don't need to see, either meant for another table, or we
                             // just sent a viewport update
                }
                existingColumnData.setAt((int) internalOffset, updatedColumnData.getAnyAt(i));
                updated.modified.add((int) internalOffset);
                i++;
            }
        }

        if (!updates.getIncludedAdditions().isEmpty()) {
            DeltaUpdates.ColumnAdditions[] serializedAdditions = updates.getSerializedAdditions();
            for (int addedColIndex =
                0; addedColIndex < serializedAdditions.length; addedColIndex++) {
                DeltaUpdates.ColumnAdditions addedColumn = serializedAdditions[addedColIndex];

                Column column =
                    columns.find((c, i1, i2) -> c.getIndex() == addedColumn.getColumnIndex());
                final JsArray<Any> addedColumnData =
                    Js.uncheckedCast(cleanData(addedColumn.getValues().getData(), column));
                final JsArray<Any> existingColumnData =
                    Js.uncheckedCast(data[addedColumn.getColumnIndex()]);
                if (addedColumnData.length == 0) {
                    continue;
                }

                int i = 0;
                OfLong it = updates.getIncludedAdditions().indexIterator();
                while (it.hasNext()) {
                    long addedOffset = it.nextLong();
                    int internalOffset = (int) (addedOffset - offset);
                    if (internalOffset < 0 || internalOffset >= maxLength) {
                        i++;
                        continue;// data we don't need to see, either meant for another table, or we
                                 // just sent a viewport update
                    }
                    assert internalOffset < existingColumnData.length;
                    existingColumnData.setAt(internalOffset, addedColumnData.getAnyAt(i));
                    updated.added.add(internalOffset);
                    i++;
                }
            }
        }

        for (Iterator<Integer> it = updated.modified.iterator(); it.hasNext();) {
            int ii = it.next();
            updated.added.remove(ii);
            updated.removed.remove(ii);
        }
        for (Iterator<Integer> it = updated.removed.iterator(); it.hasNext();) {
            int ii = it.next();
            if (updated.added.remove(ii)) {
                it.remove();
                updated.modified.add(ii);
            }
        }

        length = length + updated.added.size() - updated.removed.size();
        assert 0 <= length && length <= maxLength;

        // Viewport footprint should be small enough that we can afford to see if this update
        // corrupted our view of the world:
        assert !dataContainsNullSentinels();

        return updated;
    }

    private boolean dataContainsNullSentinels() {
        for (int i = 0; i < data.length; i++) {
            JsArray<Any> existingColumnData = Js.uncheckedCast(data[i]);
            if (existingColumnData == null) {
                continue;
            }
            for (int j = 0; j < length; ++j) {
                if (existingColumnData.getAt(j) == NULL_SENTINEL) {
                    return true;
                }
            }
        }
        return false;
    }
}
