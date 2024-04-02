//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.setinclusion;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;

import java.util.Collection;
import java.util.Iterator;

public interface SetInclusionKernel {

    void matchValues(Chunk<Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results);

    void matchValues(
            Chunk<Values> values,
            LongChunk<OrderedRowKeys> keys,
            WritableLongChunk<OrderedRowKeys> results,
            boolean inclusionOverride);

    boolean add(Object key);

    boolean remove(Object key);

    int size();

    Iterator<Object> iterator();

    static SetInclusionKernel makeKernel(ChunkType type, Collection<Object> values, boolean inclusion) {
        switch (type) {
            case Object:
                return new ObjectSetInclusionKernel(values, inclusion);
            case Char:
                return new CharSetInclusionKernel(values, inclusion);
            case Byte:
                return new ByteSetInclusionKernel(values, inclusion);
            case Short:
                return new ShortSetInclusionKernel(values, inclusion);
            case Int:
                return new IntSetInclusionKernel(values, inclusion);
            case Long:
                return new LongSetInclusionKernel(values, inclusion);
            case Double:
                return new DoubleSetInclusionKernel(values, inclusion);
            case Float:
                return new FloatSetInclusionKernel(values, inclusion);
            default:
                throw new UnsupportedOperationException();
        }
    }

    static SetInclusionKernel makeKernel(ChunkType type, boolean inclusion) {
        switch (type) {
            case Object:
                return new ObjectSetInclusionKernel(inclusion);
            case Char:
                return new CharSetInclusionKernel(inclusion);
            case Byte:
                return new ByteSetInclusionKernel(inclusion);
            case Short:
                return new ShortSetInclusionKernel(inclusion);
            case Int:
                return new IntSetInclusionKernel(inclusion);
            case Long:
                return new LongSetInclusionKernel(inclusion);
            case Double:
                return new DoubleSetInclusionKernel(inclusion);
            case Float:
                return new FloatSetInclusionKernel(inclusion);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
