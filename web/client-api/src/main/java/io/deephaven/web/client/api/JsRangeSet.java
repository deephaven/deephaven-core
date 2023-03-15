/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import io.deephaven.web.client.fu.JsIterator;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

import java.util.Arrays;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * Simple wrapper to emulate RangeSet/Index in JS, with the caveat that LongWrappers may make poor keys in plain JS.
 */
@JsType(namespace = "dh", name = "RangeSet")
public class JsRangeSet {
    private final RangeSet range;

    public static JsRangeSet ofRange(double first, double last) {
        return new JsRangeSet(RangeSet.ofRange((long) first, (long) last));
    }

    public static JsRangeSet ofItems(double[] rows) {
        long[] longs = new long[rows.length];
        for (int i = 0; i < rows.length; i++) {
            longs[i] = (long) rows[i];
        }
        return new JsRangeSet(RangeSet.ofItems(longs));
    }

    public static JsRangeSet ofRanges(JsRangeSet[] ranges) {
        RangeSet result = new RangeSet();
        for (int i = 0; i < ranges.length; i++) {
            ranges[i].range.rangeIterator().forEachRemaining(result::addRange);
        }
        return new JsRangeSet(result);
    }

    public static JsRangeSet ofSortedRanges(JsRangeSet[] ranges) {
        Range[] rangeArray = Arrays.stream(ranges).flatMap(
                r -> StreamSupport.stream(Spliterators.spliterator(r.range.rangeIterator(), Long.MAX_VALUE, 0), false))
                .toArray(Range[]::new);

        return new JsRangeSet(RangeSet.fromSortedRanges(rangeArray));
    }

    @JsIgnore
    public JsRangeSet(RangeSet range) {
        this.range = range;
    }

    public JsIterator<LongWrapper> iterator() {
        return new JsIterator<>(
                StreamSupport.longStream(Spliterators.spliterator(range.indexIterator(), Long.MAX_VALUE, 0), false)
                        .mapToObj(LongWrapper::of)
                        .iterator());
    }

    @JsProperty
    public double getSize() {
        return range.size();
    }

    @JsIgnore
    public RangeSet getRange() {
        return range;
    }
}
