package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.verify.Require;
import javax.annotation.OverridingMethodsMustInvokeSuper;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.codec.ObjectDecoder;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.sources.regioned.RegionUtilities.getDecoderBuffer;

public final class ColumnRegionObjectCodecFixed<T, ATTR extends Attributes.Any>
        implements ColumnRegionObject<T, ATTR>, Page.WithDefaults<ATTR> {

    private final Class<T> nativeType;
    private ObjectDecoder<T> decoder;
    private final int width;
    private final ColumnRegionByte<Attributes.EncodedObjects> binaryBytes;

    ColumnRegionObjectCodecFixed(@NotNull Class<T> nativeType, ObjectDecoder<T> decoder,
                                 ColumnRegionByte<Attributes.EncodedObjects> binaryBytes) {
        this.nativeType = nativeType;
        this.decoder = decoder;
        this.width = Require.neq(decoder.expectedObjectWidth(), "codec expected width",
                ObjectDecoder.VARIABLE_WIDTH_SENTINEL, "codec variable width");
        this.binaryBytes = binaryBytes;
    }

    @Override
    public T getObject(long elementIndex) {
        final long byteOffset = getRowOffset(elementIndex) * width;
        final byte[] bytesToDecode = binaryBytes.getBytes(byteOffset, getDecoderBuffer(width), 0, width);
        return decoder.decode(bytesToDecode, 0, width);
    }

    @Override
    public T getObject(@NotNull ChunkSource.FillContext context, long elementIndex) {
        final long byteOffset = getRowOffset(elementIndex) * width;
        ByteChunk<? extends Attributes.EncodedObjects> bytesChunk =
                binaryBytes.getChunk(FillContext.getBytesGetContext(context), byteOffset, byteOffset + width - 1).asByteChunk();
        return bytesChunk.applyDecoder(decoder);
    }

    @Override
    public void fillChunkAppend(@NotNull ChunkSource.FillContext fillContext, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        WritableObjectChunk<T, ? super ATTR> objectDestination = destination.asWritableObjectChunk();

        final DefaultGetContext<Attributes.EncodedObjects> binaryDataGetContext = FillContext.getBytesGetContext(fillContext);

        if (orderedKeys.getAverageRunLengthEstimate() >= 2) {
            orderedKeys.forAllLongRanges((final long rangeStartKey, final long rangeLastKey) -> {

                ByteChunk<? extends Attributes.EncodedObjects> binaryBytes =
                        this.binaryBytes.getChunk(binaryDataGetContext,
                                getRowOffset(rangeStartKey) * width,
                                (getRowOffset(rangeLastKey) + 1) * width - 1).asByteChunk();

                int size = objectDestination.size();
                for (int i = 0; i < binaryBytes.size(); i += width) {
                    objectDestination.set(size++, binaryBytes.applyDecoder(decoder, i, width));
                }
                objectDestination.setSize(size);
            });
        } else {
            orderedKeys.forAllLongs((final long key) -> {
                long offset = getRowOffset(key) * width;
                ByteChunk<? extends Attributes.EncodedObjects> binaryBytes =
                        this.binaryBytes.getChunk(binaryDataGetContext, offset, offset + width - 1).asByteChunk();
                objectDestination.add(binaryBytes.applyDecoder(decoder));
            });
        }
    }

    @Override @NotNull
    public Class<T> getNativeType() {
        return nativeType;
    }

    @Override
    public long length() {
        return binaryBytes.length() / width;
    }

    @Override @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ColumnRegionObject.super.releaseCachedResources();
        binaryBytes.releaseCachedResources();
    }

    static class FillContext extends DefaultGetContext<Attributes.EncodedObjects> implements ChunkSource.FillContext {
        public FillContext(int byteChunkCapacity) {
            super(new RegionContextHolder(), ChunkType.Byte, byteChunkCapacity);
        }

        static DefaultGetContext<Attributes.EncodedObjects> getBytesGetContext(ChunkSource.FillContext context) {
            return ((FillContext) context);
        }
    }
}
