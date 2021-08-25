package io.deephaven.web.client.api;

import io.deephaven.web.client.fu.JsIterator;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * Simple wrapper to emulate RangeSet/Index in JS, with the caveat that LongWrappers may make poor keys in plain JS.
 */
public class JsRangeSet {
    private final RangeSet range;

    @JsMethod(namespace = "dh.RangeSet", name = "ofRange")
    public static JsRangeSet ofRange(double first, double last) {
        return new JsRangeSet(RangeSet.ofRange((long) first, (long) last));
    }

    @JsMethod(namespace = "dh.RangeSet", name = "ofItems")
    public static JsRangeSet ofItems(double[] rows) {
        long[] longs = new long[rows.length];
        for (int i = 0; i < rows.length; i++) {
            longs[i] = (long) rows[i];
        }
        return new JsRangeSet(RangeSet.ofItems(longs));
    }

    @JsMethod(namespace = "dh.RangeSet", name = "ofRanges")
    public static JsRangeSet ofRanges(JsRangeSet[] ranges) {
        RangeSet result = new RangeSet();
        for (int i = 0; i < ranges.length; i++) {
            ranges[i].range.rangeIterator().forEachRemaining(result::addRange);
        }
        return new JsRangeSet(result);
    }

    @JsMethod(namespace = "dh.RangeSet", name = "ofSortedRanges")
    public static JsRangeSet ofSortedRanges(JsRangeSet[] ranges) {
        Range[] rangeArray = Arrays.stream(ranges).flatMap(
                r -> StreamSupport.stream(Spliterators.spliterator(r.range.rangeIterator(), Long.MAX_VALUE, 0), false))
                .toArray(Range[]::new);

        return new JsRangeSet(RangeSet.fromSortedRanges(rangeArray));
    }

    public JsRangeSet(RangeSet range) {
        this.range = range;
    }

    @JsMethod
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

    public RangeSet getRange() {
        return range;
    }
}
