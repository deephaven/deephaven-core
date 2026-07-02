//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.QueryConstants;

/**
 * Fills a pre-allocated {@link WritableIntChunk} with dictionary indices for one column batch. One implementation per
 * source {@link ChunkType}; avoids boxing and per-element ChunkType dispatch in the hot path.
 *
 * <p>
 * The chunk is expected to be pre-sized to the logical row count. After {@code fillIndexChunk} returns, every output
 * position corresponds to exactly one logical row: {@code QueryConstants.NULL_INT} for null rows (in
 * non-deephaven-nulls mode) and a non-negative dictionary index for non-null rows.
 */
interface DictionaryIndexKernel {

    void fillIndexChunk(
            Chunk<Values> source,
            RowSet subset,
            BarrageOptions options,
            DictionaryWriterState state,
            WritableIntChunk<Values> out);

    static DictionaryIndexKernel make(final ChunkType valuesChunkType) {
        switch (valuesChunkType) {
            case Byte:
                return ByteDictionaryIndexKernel.INSTANCE;
            case Char:
                return CharDictionaryIndexKernel.INSTANCE;
            case Short:
                return ShortDictionaryIndexKernel.INSTANCE;
            case Int:
                return IntDictionaryIndexKernel.INSTANCE;
            case Long:
                return LongDictionaryIndexKernel.INSTANCE;
            case Float:
                return FloatDictionaryIndexKernel.INSTANCE;
            case Double:
                return DoubleDictionaryIndexKernel.INSTANCE;
            default:
                return ObjectDictionaryIndexKernel.INSTANCE;
        }
    }
}


final class ByteDictionaryIndexKernel implements DictionaryIndexKernel {
    static final ByteDictionaryIndexKernel INSTANCE = new ByteDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final ByteChunk<Values> src = source.asByteChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForByte(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final byte v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_BYTE ? QueryConstants.NULL_INT : state.indexForByte(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForByte(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final byte v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_BYTE ? QueryConstants.NULL_INT : state.indexForByte(v));
                    }
                }
            }
        }
    }
}


final class CharDictionaryIndexKernel implements DictionaryIndexKernel {
    static final CharDictionaryIndexKernel INSTANCE = new CharDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final CharChunk<Values> src = source.asCharChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForChar(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final char v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : state.indexForChar(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForChar(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final char v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : state.indexForChar(v));
                    }
                }
            }
        }
    }
}


final class ShortDictionaryIndexKernel implements DictionaryIndexKernel {
    static final ShortDictionaryIndexKernel INSTANCE = new ShortDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final ShortChunk<Values> src = source.asShortChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForShort(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final short v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_SHORT ? QueryConstants.NULL_INT : state.indexForShort(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForShort(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final short v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_SHORT ? QueryConstants.NULL_INT : state.indexForShort(v));
                    }
                }
            }
        }
    }
}


final class IntDictionaryIndexKernel implements DictionaryIndexKernel {
    static final IntDictionaryIndexKernel INSTANCE = new IntDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final IntChunk<Values> src = source.asIntChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForInt(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final int v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_INT ? QueryConstants.NULL_INT : state.indexForInt(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForInt(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final int v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_INT ? QueryConstants.NULL_INT : state.indexForInt(v));
                    }
                }
            }
        }
    }
}


final class LongDictionaryIndexKernel implements DictionaryIndexKernel {
    static final LongDictionaryIndexKernel INSTANCE = new LongDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final LongChunk<Values> src = source.asLongChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForLong(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final long v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_LONG ? QueryConstants.NULL_INT : state.indexForLong(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForLong(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final long v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_LONG ? QueryConstants.NULL_INT : state.indexForLong(v));
                    }
                }
            }
        }
    }
}


final class FloatDictionaryIndexKernel implements DictionaryIndexKernel {
    static final FloatDictionaryIndexKernel INSTANCE = new FloatDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final FloatChunk<Values> src = source.asFloatChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForFloat(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final float v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_INT : state.indexForFloat(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForFloat(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final float v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_INT : state.indexForFloat(v));
                    }
                }
            }
        }
    }
}


final class DoubleDictionaryIndexKernel implements DictionaryIndexKernel {
    static final DoubleDictionaryIndexKernel INSTANCE = new DoubleDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final DoubleChunk<Values> src = source.asDoubleChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForDouble(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final double v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_DOUBLE ? QueryConstants.NULL_INT : state.indexForDouble(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForDouble(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final double v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_DOUBLE ? QueryConstants.NULL_INT : state.indexForDouble(v));
                    }
                }
            }
        }
    }
}


final class ObjectDictionaryIndexKernel implements DictionaryIndexKernel {
    static final ObjectDictionaryIndexKernel INSTANCE = new ObjectDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final ObjectChunk<Object, Values> src = source.asObjectChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            for (int i = 0; i < size; ++i) {
                final Object v = src.get(i);
                out.set(outPos++, v == null ? QueryConstants.NULL_INT : state.indexForObject(v));
            }
        } else {
            try (final RowSet.Iterator it = subset.iterator()) {
                while (it.hasNext()) {
                    final Object v = src.get((int) it.nextLong());
                    out.set(outPos++, v == null ? QueryConstants.NULL_INT : state.indexForObject(v));
                }
            }
        }
    }
}
