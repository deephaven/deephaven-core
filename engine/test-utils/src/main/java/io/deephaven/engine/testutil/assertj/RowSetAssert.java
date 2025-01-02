//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.LongArrayAssert;
import org.assertj.core.api.LongAssert;
import org.assertj.core.api.OptionalLongAssert;

import java.util.OptionalLong;

public class RowSetAssert extends AbstractAssert<RowSetAssert, RowSet> {
    public static RowSetAssert assertThat(RowSet rowSet) {
        return new RowSetAssert(rowSet);
    }

    public RowSetAssert(RowSet rowSet) {
        super(rowSet, RowSetAssert.class);
    }

    public LongArrayAssert keys() {
        final long[] keys = new long[actual.intSize()];
        actual.toRowKeyArray(keys);
        return new LongArrayAssert(keys);
    }

    public OptionalLongAssert firstRowKey() {
        final long key = actual.firstRowKey();
        return Assertions.assertThat(key == RowSequence.NULL_ROW_KEY ? OptionalLong.empty() : OptionalLong.of(key));
    }

    public OptionalLongAssert lastRowKey() {
        final long key = actual.lastRowKey();
        return Assertions.assertThat(key == RowSequence.NULL_ROW_KEY ? OptionalLong.empty() : OptionalLong.of(key));
    }

    public LongAssert find(long key) {
        return new LongAssert(actual.find(key));
    }

    public LongAssert get(long rowPosition) {
        return new LongAssert(actual.get(rowPosition));
    }

    public LongAssert size() {
        return new LongAssert(actual.size());
    }

    public RowSetAssert prev() {
        isTracking();
        return assertThat(actual.trackingCast().prev());
    }

    public void containsRange(long start, long end) {
        Assertions.assertThat(actual.containsRange(start, end)).isTrue();
    }

    public void doesNotContainsRange(long start, long end) {
        Assertions.assertThat(actual.containsRange(start, end)).isFalse();
    }

    public void overlapsRange(long start, long end) {
        Assertions.assertThat(actual.overlapsRange(start, end)).isTrue();
    }

    public void doesNotOverlapsRange(long start, long end) {
        Assertions.assertThat(actual.overlapsRange(start, end)).isFalse();
    }

    public void isFlat() {
        Assertions.assertThat(actual.isFlat()).isTrue();
    }

    public void isNotFlat() {
        Assertions.assertThat(actual.isFlat()).isFalse();
    }

    public void isEmpty() {
        Assertions.assertThat(actual.isEmpty()).isTrue();
    }

    public void isNotEmpty() {
        Assertions.assertThat(actual.isEmpty()).isFalse();
    }

    public void isTracking() {
        Assertions.assertThat(actual.isTracking()).isTrue();
    }

    public void isNotTracking() {
        Assertions.assertThat(actual.isTracking()).isFalse();
    }

    public void isWritable() {
        Assertions.assertThat(actual.isWritable()).isTrue();
    }

    public void isNotWritable() {
        Assertions.assertThat(actual.isWritable()).isFalse();
    }
}
