/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

@Category(OutOfBandTest.class)
public class QueryTableCrossJoinTest extends QueryTableCrossJoinTestBase {
    public QueryTableCrossJoinTest() {
        super(16); // default
    }
}
