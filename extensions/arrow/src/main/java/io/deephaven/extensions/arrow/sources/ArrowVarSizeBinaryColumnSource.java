package io.deephaven.extensions.arrow.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.extensions.arrow.ArrowWrapperTools;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;

/**
 * Arrow Vector: {@link VarBinaryVector}
 * Deephaven Type: byte[]
 */
public class ArrowVarSizeBinaryColumnSource extends AbstractArrowColumnSource<byte[]> implements ImmutableColumnSourceGetDefaults.ForObject<byte[]> {
    public ArrowVarSizeBinaryColumnSource(final int highBit, final @NotNull Field field,
            final @NotNull ArrowWrapperTools. Helper arrowHelper) {
        super(byte[].class, highBit, field, arrowHelper);
    }

    @Override
    public void fillChunk(final @NotNull ChunkSource. FillContext context,
            final @NotNull WritableChunk<? super Values> destination,
            final @NotNull RowSequence rowSequence) {
        final WritableObjectChunk<byte[], ? super Values> chunk = destination.asWritableObjectChunk();
        final ArrowWrapperTools.FillContext arrowContext = (ArrowWrapperTools.FillContext) context;
        chunk.setSize(0);
        fillChunk(arrowContext, rowSequence, rowKey -> chunk.add(extract(getPositionInBlock(rowKey), arrowContext.getVector(field))));
    }

    @Override
    public final byte[] get(final long rowKey) {
        try (ArrowWrapperTools.FillContext fc = (ArrowWrapperTools.FillContext) makeFillContext(1)) {
            fc.ensureLoadingBlock(getBlockNo(rowKey));
            return extract(getPositionInBlock(rowKey), fc.getVector(field));
        }
    }

    private byte[] extract(final int posInBlock, final VarBinaryVector vector) {
        return vector.getObject(posInBlock);
    }
}
