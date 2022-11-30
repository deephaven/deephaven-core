package io.deephaven.engine.testutil.generator;

import java.util.Random;
import java.util.TreeMap;

public class UniqueStringGenerator extends AbstractUniqueGenerator<String> {
    @Override
    public String nextValue(TreeMap<Long, String> values, long key, Random random) {
        return Long.toString(random.nextLong(), 'z' - 'a' + 10);
    }

    @Override
    public Class<String> getType() {
        return String.class;
    }
}
