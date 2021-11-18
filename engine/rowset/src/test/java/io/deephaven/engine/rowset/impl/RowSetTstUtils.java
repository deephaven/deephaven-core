package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;

import java.util.Random;

/**
 * Testing-related RowSet utilities
 */
public class RowSetTstUtils {

    public static WritableRowSet getRandomRowSet(long minValue, int size, Random random) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        long previous = minValue;
        for (int i = 0; i < size; i++) {
            previous += (1 + random.nextInt(100));
            builder.addKey(previous);
        }
        return builder.build();
    }
}
