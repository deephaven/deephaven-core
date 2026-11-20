//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.gwt.junit.client.GWTTestCase;
import elemental2.core.JsIIterableResult;
import elemental2.core.JsIterator;
import io.deephaven.web.shared.data.RangeSet;

public class JsRangeSetTestGwt extends GWTTestCase {

    public void testIteratorDone() {
        JsIterator<LongWrapper, Void, Void> itr = JsRangeSet.ofItems(new double[] {1, 2, 3}).iterator();
        for (int i = 1; i <= 3; i++) {
            JsIIterableResult<LongWrapper> result = itr.next();
            assertFalse("done must be false when value " + i + " is returned", result.isDone());
            assertEquals(i, result.getValue().getWrapped());
        }
        assertTrue("done must be true after the last value", itr.next().isDone());
    }

    public void testEmptyIteratorDone() {
        JsIterator<LongWrapper, Void, Void> itr = new JsRangeSet(new RangeSet()).iterator();
        assertTrue("done must be true for an empty range set", itr.next().isDone());
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenUnitTest";
    }
}
