package io.deephaven.db.v2.select;

import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.v2.SortedColumnsAttribute;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A filter that determines if a column value is between an upper and lower bound (which each may either be inclusive or
 * exclusive).
 */
public abstract class AbstractRangeFilter extends SelectFilterImpl {
    private static final Pattern decimalPattern = Pattern.compile("(-)?\\d+(?:\\.((\\d+)0*)?)?");

    protected final String columnName;
    protected final boolean upperInclusive;
    protected final boolean lowerInclusive;

    /**
     * The chunkFilter can be applied to the columns native type.
     *
     * In practice, this is for non-reinterpretable DBDateTimes.
     */
    ChunkFilter chunkFilter;
    /**
     * If the column can be be reinterpreted to a long, then we should prefer to use the longFilter instead.
     *
     * In practice, this is used for reinterpretable DBDateTimes.
     */
    ChunkFilter longFilter;

    AbstractRangeFilter(String columnName, boolean lowerInclusive, boolean upperInclusive) {
        this.columnName = columnName;
        this.upperInclusive = upperInclusive;
        this.lowerInclusive = lowerInclusive;
    }

    public static SelectFilter makeBigDecimalRange(String columnName, String val) {
        final int precision = findPrecision(val);
        final BigDecimal parsed = new BigDecimal(val);
        final BigDecimal offset = BigDecimal.valueOf(1, precision);
        final boolean positiveOrZero = parsed.signum() >= 0;

        return new ComparableRangeFilter(columnName, parsed,
                positiveOrZero ? parsed.add(offset) : parsed.subtract(offset), positiveOrZero, !positiveOrZero);
    }

    static int findPrecision(String val) {
        final Matcher m = decimalPattern.matcher(val);
        if (m.matches()) {
            final String fractionalPart = m.group(2);
            return fractionalPart == null ? 0 : fractionalPart.length();
        }

        throw new NumberFormatException("The value " + val + " is not a double");
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(columnName);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public Index filter(Index selection, Index fullSet, Table table, boolean usePrev) {
        final ColumnSource columnSource = table.getColumnSource(columnName);
        final Optional<SortingOrder> orderForColumn = SortedColumnsAttribute.getOrderForColumn(table, columnName);
        if (orderForColumn.isPresent()) {
            // do binary search for value
            return binarySearch(selection, columnSource, usePrev, orderForColumn.get().isDescending());
        }
        if (longFilter != null && columnSource.allowsReinterpret(long.class)) {
            return ChunkFilter.applyChunkFilter(selection, columnSource.reinterpret(long.class), usePrev, longFilter);
        }
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilter);
    }

    abstract Index binarySearch(Index selection, ColumnSource columnSource, boolean usePrev, boolean reverse);

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}
}
