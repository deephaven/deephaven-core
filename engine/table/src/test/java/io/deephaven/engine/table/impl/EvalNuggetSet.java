/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.tuple.ArrayTuple;
import org.junit.Assert;

import java.util.*;

public abstract class EvalNuggetSet extends EvalNugget {
    public EvalNuggetSet(String description) {
        super(description);
    }

    @Override
    public void validate(final String msg) {
        final Table expected = e();
        try {
            TableTools.show(expected);
            TableTools.show(originalValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Collection<? extends ColumnSource> sources = originalValue.getColumnSources();
        // TODO create a key for each row and go from there
        Map<ArrayTuple, Long> originalSet = new HashMap<>();
        Assert.assertEquals(expected.size(), originalValue.size());
        for (RowSet.Iterator iterator = originalValue.getRowSet().iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            Object key[] = new Object[sources.size()];
            int i = 0;
            for (ColumnSource source : sources) {
                key[i++] = source.get(next);
            }
            final ArrayTuple k = new ArrayTuple(key);

            Assert.assertEquals(msg + " k = " + k, originalSet.put(k, next), null);
        }
        sources = expected.getColumnSources();
        for (RowSet.Iterator iterator = expected.getRowSet().iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            Object key[] = new Object[sources.size()];
            int i = 0;
            for (ColumnSource source : sources) {
                key[i++] = source.get(next);
            }
            Assert.assertNotSame(msg, originalSet.remove(new ArrayTuple(key)), null);
        }

    }
}
