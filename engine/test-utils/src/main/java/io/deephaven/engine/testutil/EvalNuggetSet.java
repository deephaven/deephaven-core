//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil;

import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.tuple.ArrayTuple;
import io.deephaven.util.SafeCloseable;
import org.junit.Assert;

import java.util.*;

public abstract class EvalNuggetSet extends EvalNugget {
    public EvalNuggetSet(String description) {
        super(description);
    }

    @Override
    public void validate(final String msg) {
        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final Table expected = e();
            try {
                TableTools.show(expected);
                TableTools.show(originalValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Collection<? extends ColumnSource<?>> sources = originalValue.getColumnSources();
            // TODO create a key for each row and go from there
            final Map<ArrayTuple, Long> originalSet = new HashMap<>();
            Assert.assertEquals(expected.size(), originalValue.size());
            try (final RowSet.Iterator iterator = originalValue.getRowSet().iterator()) {
                while (iterator.hasNext()) {
                    final long next = iterator.nextLong();
                    final Object[] key = new Object[sources.size()];
                    int i = 0;
                    for (ColumnSource<?> source : sources) {
                        key[i++] = source.get(next);
                    }
                    final ArrayTuple k = new ArrayTuple(key);

                    Assert.assertNull(msg + " k = " + k, originalSet.put(k, next));
                }
            }
            sources = expected.getColumnSources();
            for (final RowSet.Iterator iterator = expected.getRowSet().iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();
                final Object[] key = new Object[sources.size()];
                int i = 0;
                for (final ColumnSource<?> source : sources) {
                    key[i++] = source.get(next);
                }
                Assert.assertNotSame(msg, originalSet.remove(new ArrayTuple(key)), null);
            }
        }
    }
}
