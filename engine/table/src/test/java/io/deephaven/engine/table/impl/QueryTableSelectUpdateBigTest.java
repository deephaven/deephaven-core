//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.assertTrue;

/**
 * Test QueryTable select and update operations.
 */
@Category(OutOfBandTest.class)
public class QueryTableSelectUpdateBigTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void test2DSelect() {
        final Table input = emptyTable(2_200_000_000L).updateView("A=(byte)7").where("ii >= 10");
        // Only row counts above TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD reach the two-dimensional column sources;
        // at or below it select() populates a flat source instead, and this test covers nothing it is named for.
        assertTrue(input.size() > InMemoryColumnSource.TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD);
        final Table selected = input.select();
        assertTableEquals(input, selected);
    }
}
