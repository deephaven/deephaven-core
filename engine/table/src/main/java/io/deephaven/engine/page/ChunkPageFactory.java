package io.deephaven.engine.page;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Factory for wrapping native arrays as {@link ChunkPage} instances.
 */
public enum ChunkPageFactory {

    // @formatter:off

    Boolean() {
        @Override
        @NotNull
        public final <ATTR extends Any> BooleanChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return BooleanChunkPage.pageWrap(beginRow, (boolean[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> BooleanChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return BooleanChunkPage.pageWrap(beginRow, (boolean[]) array, offset, capacity, mask);
        }
    },

    Char() {
        @Override
        @NotNull
        public final <ATTR extends Any> CharChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return CharChunkPage.pageWrap(beginRow, (char[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> CharChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return CharChunkPage.pageWrap(beginRow, (char[]) array, offset, capacity, mask);
        }
    },

    Byte() {
        @Override
        @NotNull
        public final <ATTR extends Any> ByteChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return ByteChunkPage.pageWrap(beginRow, (byte[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> ByteChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return ByteChunkPage.pageWrap(beginRow, (byte[]) array, offset, capacity, mask);
        }
    },

    Short() {
        @Override
        @NotNull
        public final <ATTR extends Any> ShortChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return ShortChunkPage.pageWrap(beginRow, (short[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> ShortChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return ShortChunkPage.pageWrap(beginRow, (short[]) array, offset, capacity, mask);
        }
    },

    Int() {
        @Override
        @NotNull
        public final <ATTR extends Any> IntChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return IntChunkPage.pageWrap(beginRow, (int[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> IntChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return IntChunkPage.pageWrap(beginRow, (int[]) array, offset, capacity, mask);
        }
    },

    Long() {
        @Override
        @NotNull
        public final <ATTR extends Any> LongChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return LongChunkPage.pageWrap(beginRow, (long[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> LongChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return LongChunkPage.pageWrap(beginRow, (long[]) array, offset, capacity, mask);
        }
    },

    Float() {
        @Override
        @NotNull
        public final <ATTR extends Any> FloatChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return FloatChunkPage.pageWrap(beginRow, (float[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> FloatChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return FloatChunkPage.pageWrap(beginRow, (float[]) array, offset, capacity, mask);
        }
    },

    Double() {
        @Override
        @NotNull
        public final <ATTR extends Any> DoubleChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return DoubleChunkPage.pageWrap(beginRow, (double[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> DoubleChunkPage<ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return DoubleChunkPage.pageWrap(beginRow, (double[]) array, offset, capacity, mask);
        }
    },

    Object() {
        @Override
        @NotNull
        public final <ATTR extends Any> ObjectChunkPage<?, ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final long mask) {
            return ObjectChunkPage.pageWrap(beginRow, (Object[]) array, mask);
        }
        @Override
        @NotNull
        public final <ATTR extends Any> ObjectChunkPage<?, ATTR> pageWrap(
                final long beginRow, @NotNull final Object array, final int offset, final int capacity, final long mask) {
            return ObjectChunkPage.pageWrap(beginRow, (Object[]) array, offset, capacity, mask);
        }
    };

    // @formatter:on

    private static final Map<ChunkType, ChunkPageFactory> BY_CHUNK_TYPE;
    static {
        final Map<ChunkType, ChunkPageFactory> byChunkType = new EnumMap<>(ChunkType.class);
        byChunkType.put(ChunkType.Boolean, ChunkPageFactory.Boolean);
        byChunkType.put(ChunkType.Char, ChunkPageFactory.Char);
        byChunkType.put(ChunkType.Byte, ChunkPageFactory.Byte);
        byChunkType.put(ChunkType.Short, ChunkPageFactory.Short);
        byChunkType.put(ChunkType.Int, ChunkPageFactory.Int);
        byChunkType.put(ChunkType.Long, ChunkPageFactory.Long);
        byChunkType.put(ChunkType.Float, ChunkPageFactory.Float);
        byChunkType.put(ChunkType.Double, ChunkPageFactory.Double);
        byChunkType.put(ChunkType.Object, ChunkPageFactory.Object);
        BY_CHUNK_TYPE = Collections.unmodifiableMap(byChunkType);
    }

    public static ChunkPageFactory forChunkType(@NotNull final ChunkType chunkType) {
        return BY_CHUNK_TYPE.get(chunkType);
    }

    public static ChunkPageFactory forElementType(@NotNull final Class<?> clazz) {
        return BY_CHUNK_TYPE.get(ChunkType.fromElementType(clazz));
    }

    @NotNull
    public abstract <ATTR extends Any> ChunkPage<ATTR> pageWrap(long beginRow, @NotNull Object array, long mask);

    @NotNull
    public abstract <ATTR extends Any> ChunkPage<ATTR> pageWrap(long beginRow, @NotNull Object array, int offset,
            int capacity, long mask);
}
