package io.deephaven.engine.rowset;

import io.deephaven.base.verify.Assert;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;

final class WebRowSetBuilderSequentialImpl implements RowSetBuilderSequential {
    private final RangeSet rangeSet = new RangeSet();
    @Override
    public void appendRange(long rangeFirstRowKey, long rangeLastRowKey) {
        Assert.geqZero(rangeFirstRowKey, "rangeFirstRowKey");
        Assert.leq(rangeFirstRowKey, "rangeFirstRowKey", rangeLastRowKey, "rangeLastRowKey");
        rangeSet.addRange(new Range(rangeFirstRowKey, rangeLastRowKey));
    }

    @Override
    public void appendKey(long key) {
        Assert.geqZero(key, "key");
        rangeSet.addRange(new Range(key, key));
    }

    @Override
    public void accept(long first, long last) {
        appendRange(first, last);
    }
    @Override
    public WritableRowSet build() {
        return new WebRowSetImpl(rangeSet);
    }
}
