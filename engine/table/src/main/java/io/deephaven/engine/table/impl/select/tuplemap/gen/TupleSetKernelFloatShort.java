//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTupleSetKernels or ./gradlew replicateTupleSetKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.tuplemap.gen;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.TupleMapSetKernel;
import io.deephaven.tuple.generated.FloatShortTuple;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.util.compare.ShortComparisons;
import it.unimi.dsi.fastutil.Hash;
import java.lang.Object;
import java.lang.Override;

public final class TupleSetKernelFloatShort extends TupleMapSetKernel {
    public TupleSetKernelFloatShort(ColumnSource[] keySources) {
        super(keySources, Strategy.INSTANCE);
    }

    private static int hash(float k0, short k1) {
        int hash = FloatComparisons.hashCode(k0);
        hash = hash * 31 + ShortComparisons.hashCode(k1);
        return hash;
    }

    @Override
    protected void match(Chunk<Values>[] keyChunks, LongChunk<OrderedRowKeys> rowKeys,
            WritableLongChunk<OrderedRowKeys> results, boolean inclusion) {
        final FloatChunk<Values> keys0 = keyChunks[0].asFloatChunk();
        final ShortChunk<Values> keys1 = keyChunks[1].asShortChunk();
        final Probe probe = new Probe();
        final int size = rowKeys.size();
        for (int ii = 0; ii < size; ++ii) {
            probe.k0 = keys0.get(ii);
            probe.k1 = keys1.get(ii);
            if (contains(probe) == inclusion) {
                results.add(rowKeys.get(ii));
            }
        }
    }

    private static final class Probe {
        float k0;

        short k1;
    }

    private static final class Strategy implements Hash.Strategy<Object> {
        private static final Strategy INSTANCE = new Strategy();

        @Override
        public int hashCode(Object key) {
            if (key instanceof Probe) {
                final Probe probe = (Probe) key;
                return hash(probe.k0, probe.k1);
            }
            final FloatShortTuple tuple = (FloatShortTuple) key;
            return hash(tuple.getFirstElement(), tuple.getSecondElement());
        }

        @Override
        public boolean equals(Object lhs, Object rhs) {
            if (lhs == null || rhs == null) {
                return lhs == rhs;
            }
            // The map passes a stored tuple as rhs, and a probe or a tuple being added as lhs
            final FloatShortTuple stored = (FloatShortTuple) rhs;
            if (lhs instanceof Probe) {
                final Probe probe = (Probe) lhs;
                return FloatComparisons.eq(probe.k0, stored.getFirstElement()) && ShortComparisons.eq(probe.k1, stored.getSecondElement());
            }
            final FloatShortTuple other = (FloatShortTuple) lhs;
            return FloatComparisons.eq(other.getFirstElement(), stored.getFirstElement()) && ShortComparisons.eq(other.getSecondElement(), stored.getSecondElement());
        }
    }
}
