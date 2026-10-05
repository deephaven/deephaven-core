//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTupleSetKernels or ./gradlew replicateTupleSetKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.tuplemap.gen;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.TupleMapSetKernel;
import io.deephaven.tuple.generated.ShortCharTuple;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.compare.ShortComparisons;
import it.unimi.dsi.fastutil.Hash;
import java.lang.Object;
import java.lang.Override;

public final class TupleSetKernelShortChar extends TupleMapSetKernel {
    public TupleSetKernelShortChar(ColumnSource[] keySources) {
        super(keySources, Strategy.INSTANCE);
    }

    private static int hash(short k0, char k1) {
        int hash = ShortComparisons.hashCode(k0);
        hash = hash * 31 + CharComparisons.hashCode(k1);
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
        final ShortChunk<Values> keys0 = keyChunks[0].asShortChunk();
        final CharChunk<Values> keys1 = keyChunks[1].asCharChunk();
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
        short k0;

        char k1;
    }

    private static final class Strategy implements Hash.Strategy<Object> {
        private static final Strategy INSTANCE = new Strategy();

        @Override
        public int hashCode(Object key) {
            if (key instanceof Probe) {
                final Probe probe = (Probe) key;
                return hash(probe.k0, probe.k1);
            }
            final ShortCharTuple tuple = (ShortCharTuple) key;
            return hash(tuple.getFirstElement(), tuple.getSecondElement());
        }

        @Override
        public boolean equals(Object lhs, Object rhs) {
            if (lhs == null || rhs == null) {
                return lhs == rhs;
            }
            // The map passes a stored tuple as rhs, and a probe or a tuple being added as lhs
            final ShortCharTuple stored = (ShortCharTuple) rhs;
            if (lhs instanceof Probe) {
                final Probe probe = (Probe) lhs;
                return ShortComparisons.eq(probe.k0, stored.getFirstElement()) && CharComparisons.eq(probe.k1, stored.getSecondElement());
            }
            final ShortCharTuple other = (ShortCharTuple) lhs;
            return ShortComparisons.eq(other.getFirstElement(), stored.getFirstElement()) && CharComparisons.eq(other.getSecondElement(), stored.getSecondElement());
        }
    }
}
