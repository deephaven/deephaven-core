//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTupleSetKernels or ./gradlew replicateTupleSetKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.tuplemap.gen;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.TupleMapSetKernel;
import io.deephaven.tuple.generated.ObjectObjectTuple;
import io.deephaven.util.compare.ObjectComparisons;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class TupleSetKernelObjectObject extends TupleMapSetKernel {
    public TupleSetKernelObjectObject(ColumnSource[] keySources) {
        super(keySources, Strategy.INSTANCE);
    }

    private static int hash(Object k0, Object k1) {
        int hash = ObjectComparisons.hashCode(k0);
        hash = hash * 31 + ObjectComparisons.hashCode(k1);
        return hash;
    }

    @Override
    protected boolean exportTuples(@NotNull ObjectIterator<Object> tuples, int @NotNull [] columns,
            @NotNull WritableChunk<Values>[] keyChunks) {
        WritableObjectChunk<Object, Values> keys0 = null;
        WritableObjectChunk<Object, Values> keys1 = null;
        for (int ci = 0; ci < columns.length; ++ci) {
            switch (columns[ci]) {
                case 0:
                keys0 = keyChunks[ci].asWritableObjectChunk();
                break;
                case 1:
                keys1 = keyChunks[ci].asWritableObjectChunk();
                break;
                default:
                throw new IllegalArgumentException("No key column " + columns[ci]);
            }
        }
        final int capacity = keyChunks[0].capacity();
        int exported = 0;
        while (exported < capacity && tuples.hasNext()) {
            final ObjectObjectTuple tuple = (ObjectObjectTuple) tuples.next();
            if (keys0 != null) {
                keys0.set(exported, tuple.getFirstElement());
            }
            if (keys1 != null) {
                keys1.set(exported, tuple.getSecondElement());
            }
            ++exported;
        }
        for (final WritableChunk<Values> keyChunk : keyChunks) {
            keyChunk.setSize(exported);
        }
        return exported > 0;
    }

    @Override
    protected void matchValues(@NotNull Chunk<Values>[] keyChunks,
            @NotNull LongChunk<OrderedRowKeys> rowKeys,
            @NotNull WritableLongChunk<OrderedRowKeys> results, boolean inclusion) {
        final ObjectChunk<Object, Values> keys0 = keyChunks[0].asObjectChunk();
        final ObjectChunk<Object, Values> keys1 = keyChunks[1].asObjectChunk();
        results.setSize(0);
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
        Object k0;

        Object k1;
    }

    private static final class Strategy implements Hash.Strategy<Object> {
        private static final Strategy INSTANCE = new Strategy();

        @Override
        public int hashCode(@NotNull Object key) {
            if (key instanceof Probe) {
                final Probe probe = (Probe) key;
                return hash(probe.k0, probe.k1);
            }
            final ObjectObjectTuple tuple = (ObjectObjectTuple) key;
            return hash(tuple.getFirstElement(), tuple.getSecondElement());
        }

        @Override
        public boolean equals(@Nullable Object lhs, @Nullable Object rhs) {
            if (lhs == null || rhs == null) {
                return lhs == rhs;
            }
            // The map passes a stored tuple as rhs, and a probe or a tuple being added as lhs
            final ObjectObjectTuple stored = (ObjectObjectTuple) rhs;
            if (lhs instanceof Probe) {
                final Probe probe = (Probe) lhs;
                return ObjectComparisons.eq(probe.k0, stored.getFirstElement()) && ObjectComparisons.eq(probe.k1, stored.getSecondElement());
            }
            final ObjectObjectTuple other = (ObjectObjectTuple) lhs;
            return ObjectComparisons.eq(other.getFirstElement(), stored.getFirstElement()) && ObjectComparisons.eq(other.getSecondElement(), stored.getSecondElement());
        }
    }
}
