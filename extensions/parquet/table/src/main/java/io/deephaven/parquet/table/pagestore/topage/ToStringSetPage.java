package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.stringset.HashStringSet;
import io.deephaven.stringset.StringSet;
import io.deephaven.stringset.LongBitmapStringSet;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.parquet.base.ColumnPageReader;
import io.deephaven.parquet.base.DataWithOffsets;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.IntBuffer;

public class ToStringSetPage<ATTR extends Any, STRING_ARRAY>
        extends ToPage.Wrap<ATTR, STRING_ARRAY, StringSet[]> {

    public static <ATTR extends Any> ToPage<ATTR, StringSet[]> create(@NotNull Class<?> nativeType,
            @NotNull ToPage<ATTR, ?> toPage) {
        Class<?> columnComponentType = toPage.getNativeType();

        if (!StringSet.class.isAssignableFrom(nativeType)) {
            throw new IllegalArgumentException("Native type " + nativeType + " is not a StringSet type.");
        }

        if (!String.class.isAssignableFrom(columnComponentType)) {
            throw new IllegalArgumentException("The column's component type " + columnComponentType +
                    "is not compatible with String");
        }

        Chunk<ATTR> dictionary = toPage.getDictionaryChunk();

        return dictionary != null && dictionary.size() <= 64 ? new ToStringSetPage.WithShortDictionary<>(toPage)
                : new ToStringSetPage<>(toPage);
    }

    private ToStringSetPage(ToPage<ATTR, STRING_ARRAY> toPage) {
        super(toPage);
    }

    @Override
    @NotNull
    public final Class<StringSet> getNativeType() {
        return StringSet.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final StringSet[] convertResult(Object result) {
        DataWithOffsets dataWithOffsets = (DataWithOffsets) result;
        String[] from = (String[]) toPage.convertResult(dataWithOffsets.materializeResult);
        IntBuffer offsets = dataWithOffsets.offsets;

        StringSet[] to = new StringSet[offsets.remaining()];

        int lastOffset = 0;
        for (int i = 0; offsets.hasRemaining();) {
            int nextOffset = offsets.get();
            if (nextOffset == DataWithOffsets.NULL_OFFSET) {
                to[i++] = null;
            } else {
                to[i++] = new HashStringSet(from, lastOffset, nextOffset - lastOffset);
                lastOffset = nextOffset;
            }
        }

        return to;
    }

    private static final class WithShortDictionary<ATTR extends Any, STRING_ARRAY>
            extends ToPage.Wrap<ATTR, STRING_ARRAY, StringSet[]> {

        WithShortDictionary(ToPage<ATTR, STRING_ARRAY> toPage) {
            super(toPage);
        }

        @Override
        @NotNull
        public final Class<StringSet> getNativeType() {
            return StringSet.class;
        }

        @Override
        @NotNull
        public final ChunkType getChunkType() {
            return ChunkType.Object;
        }

        @Override
        @NotNull
        public final Object getResult(ColumnPageReader columnPageReader) throws IOException {
            return toPage.getDictionaryKeysToPage().getResult(columnPageReader);
        }

        @Override
        @NotNull
        public final StringSet[] convertResult(Object result) {
            DataWithOffsets dataWithOffsets = (DataWithOffsets) result;
            int[] from = (int[]) dataWithOffsets.materializeResult;
            IntBuffer offsets = dataWithOffsets.offsets;

            StringSet[] to = new StringSet[offsets.remaining()];
            int toIndex = 0;

            int prevOffset = 0;

            while (offsets.hasRemaining()) {
                int nextOffset = offsets.get();

                if (nextOffset == DataWithOffsets.NULL_OFFSET) {
                    to[toIndex++] = null;
                } else {
                    long valueBitMask = 0;

                    while (prevOffset < nextOffset) {
                        valueBitMask |= (1L << from[prevOffset++]);
                    }

                    // noinspection unchecked
                    to[toIndex++] = new LongBitmapStringSet(toPage.getReversibleLookup(), valueBitMask);
                }

            }

            return to;
        }
    }
}
