//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.arrow.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.extensions.arrow.ArrowWrapperTools;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongConsumer;

/**
 * Base class for arrow column sources
 *
 * @param <T>
 */
public abstract class AbstractArrowColumnSource<T> extends AbstractColumnSource<T> {

    protected final int highBit;
    protected final int bitCount;
    protected final ArrowWrapperTools.ArrowTableContext arrowHelper;
    protected final Field field;

    protected AbstractArrowColumnSource(
            @NotNull final Class<T> type,
            final int highBit,
            @NotNull final Field field,
            @NotNull final ArrowWrapperTools.ArrowTableContext arrowHelper) {
        super(type);
        this.highBit = highBit;
        Assert.eq(Integer.bitCount(highBit), "Integer.bitCount(highBit)", 1, "1");
        this.bitCount = Integer.numberOfTrailingZeros(highBit);
        this.field = field;
        this.arrowHelper = arrowHelper;
    }

    @Override
    public final ArrowWrapperTools.FillContext makeFillContext(final int chunkCapacity,
            final SharedContext sharedContext) {
        return new ArrowWrapperTools.FillContext(arrowHelper, sharedContext);
    }

    protected final void fillChunk(
            @NotNull final ArrowWrapperTools.FillContext context,
            @NotNull final RowSequence rowSequence,
            @NotNull final LongConsumer rowKeyConsumer) {
        if (getBlockNo(rowSequence.firstRowKey()) == getBlockNo(rowSequence.lastRowKey())) {
            context.ensureLoadingBlock(getBlockNo(rowSequence.firstRowKey()));
            rowSequence.forAllRowKeys(rowKeyConsumer);
            return;
        }
        try (RowSequence.Iterator okIt = rowSequence.getRowSequenceIterator()) {
            while (okIt.hasMore()) {
                long nextKey = okIt.peekNextKey();
                int blockNumber = getBlockNo(nextKey);
                long lastKeyInBlock = nextKey | (highBit - 1);
                context.ensureLoadingBlock(blockNumber);
                okIt.getNextRowSequenceThrough(lastKeyInBlock).forAllRowKeys(rowKeyConsumer);
            }
        }
    }

    protected final int getBlockNo(long rowKey) {
        return LongSizedDataStructure.intSize("ArrowIntColumnSource#getInt", (rowKey >> bitCount));
    }

    protected final int getPositionInBlock(long rowKey) {
        return LongSizedDataStructure.intSize("ArrowIntColumnSource#getInt", rowKey & (highBit - 1));
    }
}
