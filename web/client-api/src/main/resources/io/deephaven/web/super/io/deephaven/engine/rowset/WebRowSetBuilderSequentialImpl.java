package io.deephaven.engine.rowset;

import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;

final class WebRowSetBuilderSequentialImpl implements RowSetBuilderSequential {
    private final RangeSet rangeSet = new RangeSet();
    @Override
    public void appendRange(long rangeFirstRowKey, long rangeLastRowKey) {
        rangeSet.addRange(new Range(rangeFirstRowKey, rangeLastRowKey));
    }

    @Override
    public void accept(long first, long last) {
        appendRange(first, last);
    }
    @Override
    public RowSet build() {
        return new WebRowSetImpl(rangeSet);
    }
}
