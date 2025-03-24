//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.arrow.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.extensions.arrow.ArrowWrapperTools;
import java.time.Instant;

import io.deephaven.time.DateTimeUtils;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;

/**
 * Arrow Vector: {@link TimeStampVector} Deephaven Type: java.time.Instant
 */
public class ArrowInstantColumnSource extends AbstractArrowColumnSource<Instant>
        implements ImmutableColumnSourceGetDefaults.ForObject<Instant> {
    private final long factor;

    public ArrowInstantColumnSource(
            final Types.MinorType minorType,
            final int highBit,
            final @NotNull Field field,
            final @NotNull ArrowWrapperTools.ArrowTableContext arrowTableContext) {
        super(Instant.class, highBit, field, arrowTableContext);
        switch (minorType) {
            case TIMESTAMPNANO:
            case TIMESTAMPNANOTZ:
                factor = 1;
                break;
            case TIMESTAMPMICRO:
            case TIMESTAMPMICROTZ:
                factor = 1_000;
                break;
            case TIMESTAMPMILLI:
            case TIMESTAMPMILLITZ:
                factor = 1_000_000;
                break;
            case TIMESTAMPSEC:
            case TIMESTAMPSECTZ:
                factor = 1_000_000_000;
                break;
            default:
                throw new IllegalArgumentException("Unsupported minor type: " + minorType);
        }
    }

    @Override
    public void fillChunk(final ChunkSource.@NotNull FillContext context,
            final @NotNull WritableChunk<? super Values> destination,
            final @NotNull RowSequence rowSequence) {
        final WritableObjectChunk<Instant, ? super Values> chunk = destination.asWritableObjectChunk();
        final ArrowWrapperTools.FillContext arrowContext = (ArrowWrapperTools.FillContext) context;
        chunk.setSize(0);
        fillChunk(arrowContext, rowSequence,
                rowKey -> chunk.add(extract(getPositionInBlock(rowKey), arrowContext.getVector(field))));
    }

    @Override
    public final Instant get(final long rowKey) {
        try (ArrowWrapperTools.FillContext fc = (ArrowWrapperTools.FillContext) makeFillContext(0)) {
            fc.ensureLoadingBlock(getBlockNo(rowKey));
            return extract(getPositionInBlock(rowKey), fc.getVector(field));
        }
    }

    private Instant extract(final int posInBlock, final TimeStampVector vector) {
        if (vector.isSet(posInBlock) == 0) {
            return null;
        }
        return DateTimeUtils.epochNanosToInstant(factor * vector.get(posInBlock));
    }
}
