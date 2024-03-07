//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import java.util.Random;

/**
 * Generates unique strings for table test columns.
 */
public class UniqueStringGenerator extends AbstractUniqueGenerator<String> {
    @Override
    public String nextValue(long key, Random random) {
        return Long.toString(random.nextLong(), 'z' - 'a' + 10);
    }

    @Override
    public Class<String> getType() {
        return String.class;
    }
}
