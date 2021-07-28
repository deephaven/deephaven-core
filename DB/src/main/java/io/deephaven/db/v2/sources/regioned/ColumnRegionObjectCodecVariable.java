package io.deephaven.db.v2.sources.regioned;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.codec.ObjectDecoder;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.sources.regioned.RegionUtilities.getDecoderBuffer;

public final class ColumnRegionObjectCodecVariable<T, ATTR extends Attributes.Any>
        implements ColumnRegionObject<T, ATTR>, Page.WithDefaults<ATTR> {

    private final ObjectDecoder<T> decoder;
    private final ColumnRegionLong<Attributes.OrderedKeyIndices> offsetData;
    private final ColumnRegionByte<Attributes.EncodedObjects> binaryData;

    ColumnRegionObjectCodecVariable(@NotNull ObjectDecoder<T> decoder,
                                    @NotNull ColumnRegionLong<Attributes.OrderedKeyIndices> offsetData,
                                    @NotNull ColumnRegionByte<Attributes.EncodedObjects> binaryData) {
        this.decoder = decoder;
        this.offsetData = offsetData;
        this.binaryData = binaryData;
    }

    @Override
    public T getObject(long elementIndex) {
        final long beginOffset = getRowOffset(elementIndex) == 0 ? 0 : offsetData.getLong(elementIndex - 1);
        final long endOffset = offsetData.getLong(elementIndex);
        final int length = (int) (endOffset - beginOffset);

        if (length == 0) {
            return null;
        }

        final byte[] bytesToDecode = binaryData.getBytes(beginOffset, getDecoderBuffer(length), 0, length);
        return decoder.decode(bytesToDecode, 0, length);
    }

    @Override
    public T getObject(@NotNull ChunkSource.FillContext context, long elementIndex) {
        ChunkSource.FillContext offsetsFillContext = FillContext.getOffsetsGetContext(context).getFillContext();
        DefaultGetContext<Attributes.EncodedObjects> bytesFillContext = FillContext.getBytesGetContext(context);

        final long beginOffset = getRowOffset(elementIndex) == 0 ? 0 : offsetData.getLong(offsetsFillContext, elementIndex - 1);
        final long endOffset = offsetData.getLong(offsetsFillContext, elementIndex);
        final int length = (int) (endOffset - beginOffset);

        if (length == 0) {
            return null;
        }

        bytesFillContext.ensureLength(length);
        ByteChunk<? extends Attributes.EncodedObjects> bytesChunk = binaryData.getChunk(bytesFillContext, beginOffset, endOffset-1).asByteChunk();
        return bytesChunk.applyDecoder(decoder);
    }

    @Override
    public void fillChunkAppend(@NotNull ChunkSource.FillContext fillContext, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        WritableObjectChunk<T, ? super ATTR> objectDestination = destination.asWritableObjectChunk();

        final DefaultGetContext<Attributes.OrderedKeyIndices> dataGetContext = FillContext.getOffsetsGetContext(fillContext);
        final DefaultGetContext<Attributes.EncodedObjects> binaryDataGetContext = FillContext.getBytesGetContext(fillContext);

        // Pretty much if we have any runs at all, we want to use the range iterator to avoid fetching successive indices twice.
        if (orderedKeys.getAverageRunLengthEstimate() >= 2) {
            orderedKeys.forAllLongRanges((final long rangeStartKey, final long rangeEndKey) -> {
                // Get this first, as it might be on a previous (cached) buffer page, because we get the offsetsIndex chunk
                long nextBeginOffset = getRowOffset(rangeStartKey) == 0 ? 0 :
                        offsetData.getLong(dataGetContext.getFillContext(), rangeStartKey - 1);

                LongChunk<? extends Attributes.OrderedKeyIndices> offsets =
                        offsetData.getChunk(dataGetContext, rangeStartKey, rangeEndKey).asLongChunk();

                for (int i = 0; i < offsets.size(); ++i) {
                    long nextEndOffset = offsets.get(i);

                    int length = (int)(nextEndOffset - nextBeginOffset);

                    if (length == 0) {
                        objectDestination.add(null);
                    } else {
                        binaryDataGetContext.ensureLength(length);
                        ByteChunk<? extends Attributes.EncodedObjects> binaryBytes =
                                binaryData.getChunk(binaryDataGetContext, nextBeginOffset, nextEndOffset - 1).asByteChunk();
                        objectDestination.add(binaryBytes.applyDecoder(decoder));

                        nextBeginOffset = nextEndOffset;
                    }
                }
            });
        } else {
            orderedKeys.forAllLongs((final long key) -> {
                long beginOffset = getRowOffset(key) == 0 ? 0 :
                        offsetData.getLong(dataGetContext.getFillContext(), key - 1);
                long endOffset = offsetData.getLong(dataGetContext.getFillContext(), key);

                int length = (int)(endOffset - beginOffset);

                if (length == 0) {
                    objectDestination.add(null);
                } else {
                    binaryDataGetContext.ensureLength(length);
                    ByteChunk<? extends Attributes.EncodedObjects> binaryBytes =
                            binaryData.getChunk(binaryDataGetContext, beginOffset, endOffset - 1).asByteChunk();

                    objectDestination.add(binaryBytes.applyDecoder(decoder));
                }
            });
        }
    }

    @Override
    public long length() {
        return offsetData.length();
    }

    @Override @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ColumnRegionObject.super.releaseCachedResources();
        offsetData.releaseCachedResources();
        binaryData.releaseCachedResources();
    }

    static class FillContext extends ColumnRegionObjectCodecFixed.FillContext {

        DefaultGetContext<Attributes.OrderedKeyIndices> offsetsGetContext;

        public FillContext(int byteChunkCapacity, int offsetChunkCapacity) {
            super(byteChunkCapacity);
            offsetsGetContext = new DefaultGetContext<>(new RegionContextHolder(), ChunkType.Long, offsetChunkCapacity);
        }

        static DefaultGetContext<Attributes.OrderedKeyIndices> getOffsetsGetContext(ChunkSource.FillContext context) {
            return ((FillContext) context).offsetsGetContext;
        }

        @Override
        public void close() {
            super.close();
            offsetsGetContext.close();
        }
    }
}
