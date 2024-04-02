//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateArrowColumnSources or "./gradlew generateArrowColumnSources" to regenerate
//
// @formatter:off
package io.deephaven.extensions.arrow.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.extensions.arrow.ArrowWrapperTools;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;

/**
 * Arrow Vector: {@link VarCharVector}
 * Deephaven Type: java.lang.String
 */
public class ArrowStringColumnSource extends AbstractArrowColumnSource<String> implements ImmutableColumnSourceGetDefaults.ForObject<String> {
    public ArrowStringColumnSource(final int highBit, final @NotNull Field field,
            final ArrowWrapperTools. @NotNull ArrowTableContext arrowTableContext) {
        super(String.class, highBit, field, arrowTableContext);
    }

    @Override
    public void fillChunk(final ChunkSource. @NotNull FillContext context,
            final @NotNull WritableChunk<? super Values> destination,
            final @NotNull RowSequence rowSequence) {
        final WritableObjectChunk<String, ? super Values> chunk = destination.asWritableObjectChunk();
        final ArrowWrapperTools.FillContext arrowContext = (ArrowWrapperTools.FillContext) context;
        chunk.setSize(0);
        fillChunk(arrowContext, rowSequence, rowKey -> chunk.add(extract(getPositionInBlock(rowKey), arrowContext.getVector(field))));
    }

    @Override
    public final String get(final long rowKey) {
        try (ArrowWrapperTools.FillContext fc = (ArrowWrapperTools.FillContext) makeFillContext(0)) {
            fc.ensureLoadingBlock(getBlockNo(rowKey));
            return extract(getPositionInBlock(rowKey), fc.getVector(field));
        }
    }

    private String extract(final int posInBlock, final VarCharVector vector) {
        return vector.isSet(posInBlock) == 0 ? null : new String(vector.get(posInBlock));
    }
}
