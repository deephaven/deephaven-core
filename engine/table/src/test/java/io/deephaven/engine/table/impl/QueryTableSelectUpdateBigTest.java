/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

/**
 * Test QueryTable select and update operations.
 */
@Category(OutOfBandTest.class)
public class QueryTableSelectUpdateBigTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void test2DSelect() {
        final Table input = emptyTable(3_000_000_000L).updateView("A=(byte)7").where("ii >= 10");
        final Table selected = input.select();
        assertTableEquals(input, selected);
    }
}
