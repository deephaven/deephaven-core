package io.deephaven.engine.v2.utils;

import gnu.trove.procedure.TLongProcedure;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.util.LongChunkIterator;
import io.deephaven.engine.v2.sources.chunk.util.LongChunkRangeIterator;

import java.util.PrimitiveIterator;

/**
 * Builder interface for {@link RowSet} construction in strict sequential order.
 */
public interface RowSetBuilderSequential extends TLongProcedure, LongRangeConsumer {

    /**
     * Hint to call, but if called, (a) should be called before providing any values, and (b) no value should be
     * provided outside of the domain. Implementations may be able to use this information to improve memory
     * utilization. Either of the arguments may be given as RowSet.NULL_KEY, indicating that the respective value is not
     * known.
     *
     * @param minRowKey The minimum row key to be provided, or {@link RowSet#NULL_ROW_KEY} if not known
     * @param maxRowKey The maximum row key to be provided, or {@link RowSet#NULL_ROW_KEY} if not known
     */
    default void setDomain(long minRowKey, long maxRowKey) {}

    MutableRowSet build();

    void appendKey(long rowKey);

    void appendRange(long rangeFirstRowKey, long rangeLastRowKey);

    default void appendKeys(PrimitiveIterator.OfLong it) {
        while (it.hasNext()) {
            appendKey(it.nextLong());
        }
    }

    default void appendOrderedRowKeysChunk(final LongChunk<Attributes.OrderedRowKeys> chunk) {
        appendKeys(new LongChunkIterator(chunk));
    }

    default void appendRanges(final LongRangeIterator it) {
        while (it.hasNext()) {
            it.next();
            appendRange(it.start(), it.end());
        }
    }

    default void appendOrderedRowKeyRangesChunk(final LongChunk<Attributes.OrderedRowKeyRanges> chunk) {
        appendRanges(new LongChunkRangeIterator(chunk));
    }

    @Override
    default boolean execute(final long value) {
        appendKey(value);
        return true;
    }

    /**
     * Appends a {@link RowSequence} to this builder.
     *
     * @param rowSequence The {@link RowSequence} to append
     */
    default void appendRowSet(final RowSequence rowSequence) {
        rowSequence.forAllLongRanges(this::appendRange);
    }

    /**
     * Appends a {@link RowSequence} shifted by the provided offset to this builder.
     *
     * @param rowSequence The {@link RowSequence} to append
     * @param offset An offset to apply to every range in the RowSet
     */
    default void appendRowSetWithOffset(final RowSequence rowSequence, final long offset) {
        rowSequence.forAllLongRanges((s, e) -> appendRange(s + offset, e + offset));
    }
}
