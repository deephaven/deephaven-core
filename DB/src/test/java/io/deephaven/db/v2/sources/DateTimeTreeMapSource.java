package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * DateTime column source that wraps and delegates the storage to an {@code TreeMapSource<Long>}. This also provides an
 * interface so this column can be interpreted as a long column (through UnboxedDateTimeTreeMapSource).
 */
public class DateTimeTreeMapSource extends AbstractColumnSource<DBDateTime>
        implements MutableColumnSourceGetDefaults.ForObject<DBDateTime> {

    private final TreeMapSource<Long> treeMapSource;
    private final UnboxedDateTimeTreeMapSource alternateColumnSource;

    /**
     * Create a new DateTimeTreeMapSource with no initial data.
     */
    public DateTimeTreeMapSource() {
        super(DBDateTime.class);
        this.treeMapSource = new TreeMapSource<>(Long.class);
        this.alternateColumnSource = new UnboxedDateTimeTreeMapSource(this, treeMapSource);
    }

    /**
     * Create a new DateTimeTreeMapSource with the given index and data.
     *
     * @param index The row indexes for the initial data
     * @param data The initial data
     */
    public DateTimeTreeMapSource(Index index, DBDateTime[] data) {
        super(DBDateTime.class);
        this.treeMapSource = new TreeMapSource<>(Long.class, index, mapData(data));
        this.alternateColumnSource = new UnboxedDateTimeTreeMapSource(this, treeMapSource);
    }

    /**
     * Create a new DateTimeTreeMapSource with the given index and data.
     *
     * @param index The row indexes for the initial data
     * @param data The initial data
     */
    public DateTimeTreeMapSource(Index index, long[] data) {
        super(DBDateTime.class);
        final Long[] boxedData = ArrayUtils.getBoxedArray(data);
        this.treeMapSource = new TreeMapSource<>(Long.class, index, boxedData);
        this.alternateColumnSource = new UnboxedDateTimeTreeMapSource(this, treeMapSource);
    }

    private Long[] mapData(DBDateTime[] data) {
        return Arrays.stream(data).map(dt -> {
            if (dt == null) {
                return QueryConstants.NULL_LONG;
            }
            return dt.getNanos();
        }).toArray(Long[]::new);
    }

    public void add(Index index, DBDateTime[] data) {
        treeMapSource.add(index, mapData(data));
    }

    public void add(Index index, Long[] data) {
        treeMapSource.add(index, data);
    }

    public void remove(Index index) {
        treeMapSource.remove(index);
    }

    public void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta) {
        treeMapSource.shift(startKeyInclusive, endKeyInclusive, shiftDelta);
    }

    @Override
    public DBDateTime get(long index) {
        final Long v = treeMapSource.get(index);
        return v == null ? null : new DBDateTime(v);
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
    public DBDateTime getPrev(long index) {
        final Long v = treeMapSource.getPrev(index);
        return v == null ? null : new DBDateTime(v);
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
