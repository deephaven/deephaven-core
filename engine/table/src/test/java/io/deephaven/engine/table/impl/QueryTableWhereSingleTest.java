//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.test.types.OutOfBandTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category(OutOfBandTest.class)
public class QueryTableWhereSingleTest extends QueryTableWhereTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // these are reset in parent class
        QueryTable.FORCE_PARALLEL_WHERE = false;
        QueryTable.DISABLE_PARALLEL_WHERE = true;
    }
}
