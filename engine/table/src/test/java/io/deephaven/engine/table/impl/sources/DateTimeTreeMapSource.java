package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.time.DateTime;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * DateTime column source that wraps and delegates the storage to an {@code TreeMapSource<Long>}. This also provides an
 * interface so this column can be interpreted as a long column (through UnboxedDateTimeTreeMapSource).
 */
public class DateTimeTreeMapSource extends AbstractColumnSource<DateTime>
        implements MutableColumnSourceGetDefaults.ForObject<DateTime> {

    private final TreeMapSource<Long> treeMapSource;
    private final UnboxedDateTimeTreeMapSource alternateColumnSource;

    /**
     * Create a new DateTimeTreeMapSource with no initial data.
     */
    public DateTimeTreeMapSource() {
        super(DateTime.class);
        this.treeMapSource = new TreeMapSource<>(Long.class);
        this.alternateColumnSource = new UnboxedDateTimeTreeMapSource(this, treeMapSource);
    }

    /**
     * Create a new DateTimeTreeMapSource with the given rowSet and data.
     *
     * @param rowSet The row indexes for the initial data
     * @param data The initial data
     */
    public DateTimeTreeMapSource(RowSet rowSet, DateTime[] data) {
        super(DateTime.class);
        this.treeMapSource = new TreeMapSource<>(Long.class, rowSet, mapData(data));
        this.alternateColumnSource = new UnboxedDateTimeTreeMapSource(this, treeMapSource);
    }

    /**
     * Create a new DateTimeTreeMapSource with the given row set and data.
     *
     * @param rowSet The row keys for the initial data
     * @param data The initial data
     */
    public DateTimeTreeMapSource(RowSet rowSet, long[] data) {
        super(DateTime.class);
        final Long[] boxedData = ArrayTypeUtils.getBoxedArray(data);
        this.treeMapSource = new TreeMapSource<>(Long.class, rowSet, boxedData);
        this.alternateColumnSource = new UnboxedDateTimeTreeMapSource(this, treeMapSource);
    }

    private Long[] mapData(DateTime[] data) {
        return Arrays.stream(data).map(dt -> {
            if (dt == null) {
                return QueryConstants.NULL_LONG;
            }
            return dt.getNanos();
        }).toArray(Long[]::new);
    }

    public void add(RowSet rowSet, DateTime[] data) {
        treeMapSource.add(rowSet, mapData(data));
    }

    public void add(RowSet rowSet, Long[] data) {
        treeMapSource.add(rowSet, data);
    }

    public void remove(RowSet rowSet) {
        treeMapSource.remove(rowSet);
    }

    public void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta) {
        treeMapSource.shift(startKeyInclusive, endKeyInclusive, shiftDelta);
    }

    @Override
    public DateTime get(long index) {
        final Long v = treeMapSource.get(index);
        return v == null ? null : new DateTime(v);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public long getLong(long index) {
        return treeMapSource.getLong(index);
    }

    @Override
    public DateTime getPrev(long index) {
        final Long v = treeMapSource.getPrev(index);
        return v == null ? null : new DateTime(v);
    }

    @Override
    public long getPrevLong(long index) {
        return treeMapSource.getPrevLong(index);
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }
}
