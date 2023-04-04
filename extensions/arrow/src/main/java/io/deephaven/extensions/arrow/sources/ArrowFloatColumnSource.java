/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit GenerateArrowColumnSources and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.extensions.arrow.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.extensions.arrow.ArrowWrapperTools;
import io.deephaven.util.QueryConstants;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;

/**
 * Arrow Vector: {@link Float4Vector}
 * Deephaven Type: float
 */
public class ArrowFloatColumnSource extends AbstractArrowColumnSource<Float> implements ImmutableColumnSourceGetDefaults.ForFloat {
    public ArrowFloatColumnSource(final int highBit, final @NotNull Field field,
            final ArrowWrapperTools. @NotNull ArrowTableContext arrowTableContext) {
        super(float.class, highBit, field, arrowTableContext);
    }

    @Override
    public void fillChunk(final ChunkSource. @NotNull FillContext context,
            final @NotNull WritableChunk<? super Values> destination,
            final @NotNull RowSequence rowSequence) {
        final WritableFloatChunk<? super Values> chunk = destination.asWritableFloatChunk();
        final ArrowWrapperTools.FillContext arrowContext = (ArrowWrapperTools.FillContext) context;
        chunk.setSize(0);
        fillChunk(arrowContext, rowSequence, rowKey -> chunk.add(extract(getPositionInBlock(rowKey), arrowContext.getVector(field))));
    }

    @Override
    public final float getFloat(final long rowKey) {
        try (ArrowWrapperTools.FillContext fc = (ArrowWrapperTools.FillContext) makeFillContext(0)) {
            fc.ensureLoadingBlock(getBlockNo(rowKey));
            return extract(getPositionInBlock(rowKey), fc.getVector(field));
        }
    }

    private float extract(final int posInBlock, final Float4Vector vector) {
        return vector.isSet(posInBlock) == 0 ? QueryConstants.NULL_FLOAT : vector.get(posInBlock);
    }
}
