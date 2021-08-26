package io.deephaven.web.shared.data;

import java.io.Serializable;
import java.util.BitSet;

public class Viewport implements Serializable {

    private final RangeSet rows;
    private final BitSet columns;

    public Viewport() {
        this(new RangeSet(), new BitSet());
    }

    public Viewport(RangeSet rows, BitSet columns) {
        this.rows = rows;
        this.columns = columns;
    }

    public Viewport merge(Viewport other) {
        RangeSet mergedRows = new RangeSet();
        rows.rangeIterator().forEachRemaining(mergedRows::addRange);
        other.rows.rangeIterator().forEachRemaining(mergedRows::addRange);

        BitSet mergedColumns = new BitSet();
        mergedColumns.or(columns);
        mergedColumns.or(other.columns);

        return new Viewport(mergedRows, mergedColumns);
    }

    public RangeSet getRows() {
        return rows;
    }

    public BitSet getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final Viewport viewport = (Viewport) o;

        if (!rows.equals(viewport.rows))
            return false;
        return columns.equals(viewport.columns);
    }

    @Override
    public int hashCode() {
        int result = rows.hashCode();
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Viewport{" +
                "rows=" + rows +
                ", columns=" + columns +
                '}';
    }

    public boolean isEmpty() {
        return rows.size() == 0 && columns.isEmpty();
    }
}
