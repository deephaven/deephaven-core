//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.gwt.junit.client.GWTTestCase;
import io.deephaven.web.client.fu.JsIterator;
import io.deephaven.web.shared.data.RangeSet;
import jsinterop.base.Js;

public class JsRangeSetTestGwt extends GWTTestCase {

    public void testIteratorDone() {
        JsIterator<LongWrapper> itr = JsRangeSet.ofItems(new double[] {1, 2, 3}).iterator();
        for (int i = 1; i <= 3; i++) {
            JsIterator.JsIIterableResult<LongWrapper> result = next(itr);
            assertFalse("done must be false when value " + i + " is returned", result.isDone());
            assertEquals(i, result.getValue().getWrapped());
        }
        assertTrue("done must be true after the last value", next(itr).isDone());
    }

    public void testEmptyIteratorDone() {
        JsIterator<LongWrapper> itr = new JsRangeSet(new RangeSet()).iterator();
        assertTrue("done must be true for an empty range set", next(itr).isDone());
    }

    /** The result is a plain JS object, so read it through the native interface. */
    private static <T> JsIterator.JsIIterableResult<T> next(JsIterator<T> itr) {
        return Js.uncheckedCast(itr.next());
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenUnitTest";
    }
}
