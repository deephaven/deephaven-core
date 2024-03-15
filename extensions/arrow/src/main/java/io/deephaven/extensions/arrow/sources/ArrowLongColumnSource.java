//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateArrowColumnSources or "./gradlew generateArrowColumnSources" to regenerate
//
// @formatter:off
package io.deephaven.extensions.arrow.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.extensions.arrow.ArrowWrapperTools;
import io.deephaven.util.QueryConstants;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;

/**
 * Arrow Vector: {@link BigIntVector}
 * Deephaven Type: long
 */
public class ArrowLongColumnSource extends AbstractArrowColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong {
    public ArrowLongColumnSource(final int highBit, final @NotNull Field field,
            final ArrowWrapperTools. @NotNull ArrowTableContext arrowTableContext) {
        super(long.class, highBit, field, arrowTableContext);
    }

    @Override
    public void fillChunk(final ChunkSource. @NotNull FillContext context,
            final @NotNull WritableChunk<? super Values> destination,
            final @NotNull RowSequence rowSequence) {
        final WritableLongChunk<? super Values> chunk = destination.asWritableLongChunk();
        final ArrowWrapperTools.FillContext arrowContext = (ArrowWrapperTools.FillContext) context;
        chunk.setSize(0);
        fillChunk(arrowContext, rowSequence, rowKey -> chunk.add(extract(getPositionInBlock(rowKey), arrowContext.getVector(field))));
    }

    @Override
    public final long getLong(final long rowKey) {
        try (ArrowWrapperTools.FillContext fc = (ArrowWrapperTools.FillContext) makeFillContext(0)) {
            fc.ensureLoadingBlock(getBlockNo(rowKey));
            return extract(getPositionInBlock(rowKey), fc.getVector(field));
        }
    }

    private long extract(final int posInBlock, final BigIntVector vector) {
        return vector.isSet(posInBlock) == 0 ? QueryConstants.NULL_LONG : vector.get(posInBlock);
    }
}
