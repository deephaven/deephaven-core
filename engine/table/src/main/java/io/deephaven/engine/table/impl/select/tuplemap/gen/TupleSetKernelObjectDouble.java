//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTupleSetKernels or ./gradlew replicateTupleSetKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.tuplemap.gen;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.TupleMapSetKernel;
import io.deephaven.tuple.generated.ObjectDoubleTuple;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.ObjectComparisons;
import it.unimi.dsi.fastutil.Hash;
import java.lang.Object;
import java.lang.Override;

public final class TupleSetKernelObjectDouble extends TupleMapSetKernel {
    public TupleSetKernelObjectDouble(ColumnSource[] keySources) {
        super(keySources, Strategy.INSTANCE);
    }

    private static int hash(Object k0, double k1) {
        int hash = ObjectComparisons.hashCode(k0);
        hash = hash * 31 + DoubleComparisons.hashCode(k1);
        return hash;
    }

    @Override
    protected Object makeProbe() {
        return new Probe();
    }

    @Override
    protected void match(Object probeObject, Chunk<Values>[] keyChunks,
            LongChunk<OrderedRowKeys> rowKeys, WritableLongChunk<OrderedRowKeys> results,
            boolean inclusion) {
        final ObjectChunk<Object, Values> keys0 = keyChunks[0].asObjectChunk();
        final DoubleChunk<Values> keys1 = keyChunks[1].asDoubleChunk();
        final Probe probe = (Probe) probeObject;
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
        Object k0;

        double k1;
    }

    private static final class Strategy implements Hash.Strategy<Object> {
        private static final Strategy INSTANCE = new Strategy();

        @Override
        public int hashCode(Object key) {
            if (key instanceof Probe) {
                final Probe probe = (Probe) key;
                return hash(probe.k0, probe.k1);
            }
            final ObjectDoubleTuple tuple = (ObjectDoubleTuple) key;
            return hash(tuple.getFirstElement(), tuple.getSecondElement());
        }

        @Override
        public boolean equals(Object lhs, Object rhs) {
            if (lhs == null || rhs == null) {
                return lhs == rhs;
            }
            // The map passes a stored tuple as rhs, and a probe or a tuple being added as lhs
            final ObjectDoubleTuple stored = (ObjectDoubleTuple) rhs;
            if (lhs instanceof Probe) {
                final Probe probe = (Probe) lhs;
                return DoubleComparisons.eq(probe.k1, stored.getSecondElement()) && ObjectComparisons.eq(probe.k0, stored.getFirstElement());
            }
            final ObjectDoubleTuple other = (ObjectDoubleTuple) lhs;
            return DoubleComparisons.eq(other.getSecondElement(), stored.getSecondElement()) && ObjectComparisons.eq(other.getFirstElement(), stored.getFirstElement());
        }
    }
}
