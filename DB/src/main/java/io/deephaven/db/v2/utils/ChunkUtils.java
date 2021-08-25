/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.exceptions.SizeException;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.Objects;

public class ChunkUtils {
    private static final int COPY_DATA_CHUNK_SIZE = 16384;

    /**
     * Generates a {@link LongChunk<OrderedKeyRanges>} from {@link LongChunk<OrderedKeyIndices>}
     * chunk.
     * 
     * @param chunk the chunk to convert
     * @return the generated chunk
     */
    public static WritableLongChunk<OrderedKeyRanges> convertToOrderedKeyRanges(
        final LongChunk<OrderedKeyIndices> chunk) {
        return convertToOrderedKeyRanges(chunk, Chunk.MAXIMUM_SIZE);
    }

    @VisibleForTesting
    static WritableLongChunk<OrderedKeyRanges> convertToOrderedKeyRanges(
        final LongChunk<OrderedKeyIndices> chunk,
        final long maxChunkSize) {
        if (chunk.size() == 0) {
            return WritableLongChunk.makeWritableChunk(0);
        }

        // First we'll count the number of ranges so that we can allocate the exact amount of space
        // needed.
        long numRanges = 1;
        for (int idx = 1; idx < chunk.size(); ++idx) {
            if (chunk.get(idx - 1) + 1 != chunk.get(idx)) {
                ++numRanges;
            }
        }

        final long newSize = numRanges * 2L;
        if (newSize > maxChunkSize) {
            throw new SizeException("Cannot expand KeyIndices Chunk into KeyRanges Chunk.", newSize,
                maxChunkSize);
        }

        final WritableLongChunk<OrderedKeyRanges> newChunk =
            WritableLongChunk.makeWritableChunk((int) newSize);

        convertToOrderedKeyRanges(chunk, newChunk);

        return newChunk;
    }

    /**
     * Fills {@code OrderedKeyRanges} into {@code dest} from the provided {@code chunk} and
     * specified source range.
     * 
     * @param chunk the chunk to convert
     * @param dest the chunk to fill with ranges
     */
    public static void convertToOrderedKeyRanges(final LongChunk<OrderedKeyIndices> chunk,
        final WritableLongChunk<OrderedKeyRanges> dest) {
        int destOffset = 0;
        if (chunk.size() == 0) {
            dest.setSize(destOffset);
            return;
        }

        int srcOffset = 0;
        dest.set(destOffset++, chunk.get(srcOffset));
        for (++srcOffset; srcOffset < chunk.size(); ++srcOffset) {
            if (chunk.get(srcOffset - 1) + 1 != chunk.get(srcOffset)) {
                // we now know that the currently open range ends at srcOffset - 1
                dest.set(destOffset++, chunk.get(srcOffset - 1));
                dest.set(destOffset++, chunk.get(srcOffset));
            }
        }
        dest.set(destOffset++, chunk.get(srcOffset - 1));

        dest.setSize(destOffset);
    }

    /**
     * Generates a {@link LongChunk<OrderedKeyIndices>} from {@link LongChunk<OrderedKeyRanges>}
     * chunk.
     * 
     * @param chunk the chunk to convert
     * @return the generated chunk
     */
    public static LongChunk<OrderedKeyIndices> convertToOrderedKeyIndices(
        final LongChunk<OrderedKeyRanges> chunk) {
        return convertToOrderedKeyIndices(0, chunk);
    }

    /**
     * Generates a {@link LongChunk<OrderedKeyIndices>} from {@link LongChunk<OrderedKeyRanges>}
     * chunk.
     * 
     * @param srcOffset the offset into {@code chunk} to begin including in the generated chunk
     * @param chunk the chunk to convert
     * @return the generated chunk
     */
    public static LongChunk<OrderedKeyIndices> convertToOrderedKeyIndices(int srcOffset,
        final LongChunk<OrderedKeyRanges> chunk) {
        srcOffset += srcOffset % 2; // ensure that we are using the correct range edges

        long numElements = 0;
        for (int idx = 0; idx < chunk.size(); idx += 2) {
            numElements += chunk.get(idx + 1) - chunk.get(idx) + 1;
        }

        // Note that maximum range is [0, Long.MAX_VALUE] and all ranges are non-overlapping.
        // Therefore we will never
        // overflow past Long.MIN_VALUE.
        if (numElements < 0 || numElements > Chunk.MAXIMUM_SIZE) {
            throw new SizeException(
                "Cannot expand OrderedKeyRanges Chunk into OrderedKeyIndices Chunk.", numElements,
                Chunk.MAXIMUM_SIZE);
        }

        final WritableLongChunk<OrderedKeyIndices> newChunk =
            WritableLongChunk.makeWritableChunk((int) numElements);
        convertToOrderedKeyIndices(srcOffset, chunk, newChunk, 0);
        return newChunk;
    }

    /**
     * Generates a {@link LongChunk<OrderedKeyIndices>} from {@link LongChunk<OrderedKeyRanges>}
     * chunk.
     * 
     * @param srcOffset the offset into {@code chunk} to begin including in the generated chunk
     * @param chunk the chunk to convert
     * @param dest the chunk to fill with indices
     */
    public static void convertToOrderedKeyIndices(int srcOffset,
        final LongChunk<OrderedKeyRanges> chunk,
        final WritableLongChunk<OrderedKeyIndices> dest, int destOffset) {
        srcOffset += srcOffset & 1; // ensure that we are using the correct range edges

        for (int idx = srcOffset; idx + 1 < chunk.size() && destOffset < dest.size(); idx += 2) {
            final long start = chunk.get(idx);
            final long range = chunk.get(idx + 1) - start + 1; // note that due to checks above,
                                                               // range cannot overflow
            for (long jdx = 0; jdx < range && destOffset < dest.size(); ++jdx) {
                dest.set(destOffset++, start + jdx);
            }
        }

        dest.setSize(destOffset);
    }

    /**
     * Produce a pretty key for error messages from an element within parallel chunks.
     */
    public static String extractKeyStringFromChunks(ChunkType[] keyChunkTypes,
        Chunk<Attributes.Values>[] chunks, int chunkPosition) {
        final StringBuilder builder = new StringBuilder();
        if (chunks.length != 1) {
            builder.append("[");
        }
        for (int ii = 0; ii < chunks.length; ++ii) {
            if (ii > 0) {
                builder.append(", ");
            }
            extractStringOne(chunkPosition, builder, keyChunkTypes[ii], chunks[ii]);
        }
        if (chunks.length != 1) {
            builder.append("]");
        }
        return builder.toString();
    }


    /**
     * Produce a pretty key for error messages from an element within parallel chunks.
     */
    public static String extractKeyStringFromChunk(ChunkType keyChunkType,
        Chunk<? extends Attributes.Values> chunk, int chunkPosition) {
        final StringBuilder builder = new StringBuilder();
        extractStringOne(chunkPosition, builder, keyChunkType, chunk);
        return builder.toString();
    }

    /**
     * Produce a pretty key for error messages from an element within parallel chunks.
     */
    public static String extractKeyStringFromChunk(Chunk<? extends Attributes.Values> chunk,
        int chunkPosition) {
        return extractKeyStringFromChunk(chunk.getChunkType(), chunk, chunkPosition);
    }

    private static void extractStringOne(int chunkPosition, StringBuilder builder,
        ChunkType keyChunkType, Chunk<? extends Attributes.Values> chunk) {
        switch (keyChunkType) {
            case Boolean:
                builder.append(chunk.asBooleanChunk().get(chunkPosition));
                break;
            case Byte:
                final byte byteValue = chunk.asByteChunk().get(chunkPosition);
                if (QueryConstants.NULL_BYTE == byteValue) {
                    builder.append("null");
                } else {
                    builder.append(byteValue);
                }
                break;
            case Char:
                final char charValue = chunk.asCharChunk().get(chunkPosition);
                if (QueryConstants.NULL_CHAR == charValue) {
                    builder.append("null");
                } else {
                    builder.append(charValue);
                }
                break;
            case Int:
                final int intValue = chunk.asIntChunk().get(chunkPosition);
                if (QueryConstants.NULL_INT == intValue) {
                    builder.append("null");
                } else {
                    builder.append(intValue);
                }
                break;
            case Short:
                final short shortValue = chunk.asShortChunk().get(chunkPosition);
                if (QueryConstants.NULL_SHORT == shortValue) {
                    builder.append("null");
                } else {
                    builder.append(shortValue);
                }
                break;
            case Long:
                final long longValue = chunk.asLongChunk().get(chunkPosition);
                if (QueryConstants.NULL_LONG == longValue) {
                    builder.append("null");
                } else {
                    builder.append(longValue);
                }
                break;
            case Float:
                final float floatValue = chunk.asFloatChunk().get(chunkPosition);
                if (QueryConstants.NULL_FLOAT == floatValue) {
                    builder.append("null");
                } else {
                    builder.append(floatValue);
                }
                break;
            case Double:
                final double doubleValue = chunk.asDoubleChunk().get(chunkPosition);
                if (QueryConstants.NULL_DOUBLE == doubleValue) {
                    builder.append("null");
                } else {
                    builder.append(doubleValue);
                }
                break;
            case Object:
                // Objects.toString is unnecessary; the builder will do it for us
                builder.append(chunk.asObjectChunk().get(chunkPosition));
                break;
        }
    }

    public static void checkSliceArgs(int size, int offset, int capacity) {
        if (offset < 0 || offset > size || capacity < 0 || capacity > size - offset) {
            throw new IllegalArgumentException(
                String.format("New slice offset %d, capacity %d is incompatible with size %d",
                    offset, capacity, size));
        }
    }

    public static void checkArrayArgs(int arrayLength, int offset, int capacity) {
        if (offset < 0 || capacity < 0 || offset + capacity > arrayLength) {
            throw new IllegalArgumentException(
                String.format("offset %d, capacity %d is incompatible with array of length %d",
                    offset, capacity, arrayLength));
        }
    }

    /**
     * Determines, when copying data from the source array to the dest array, whether the copying
     * should proceed in the forward or reverse direction in order to be safe. The issue is that
     * (like memmove), care needs to be taken when the arrays are the same and the ranges overlap.
     * In some cases (like when the arrays are different), either direction will do; in those cases
     * we will recommend copying in the forward direction for performance reasons. When srcArray and
     * destArray refer to the array, one of these five cases applies:
     *
     * <p>
     * 
     * <pre>
     * Case 1: Source starts to the left of dest, and does not overlap. Recommend copying in forward direction.
     * SSSSS
     *      DDDDD
     *
     * Case 2: Source starts to the left of dest, and partially overlaps. MUST copy in the REVERSE direction.
     * SSSSS
     *    DDDDD
     *
     * Case 3: Source is on top of dest. Recommend copying in forward direction (really, you should do nothing).
     * SSSSS
     * DDDDD
     *
     * Case 4: Source starts to the right of dest, and partially overlaps. MUST copy in the FORWARD direction.
     *   SSSSS
     * DDDDD
     *
     * Case 5: Source starts to the right of dest, and does not overlap. Recommend copying in the forward direction.
     *      SSSSS
     * DDDDD
     * </pre>
     *
     * Note that case 2 is the only case where you need to copy backwards.
     *
     * @param srcArray The source array
     * @param srcOffset The starting offset in the srcArray
     * @param destArray The destination array
     * @param destOffset The starting offset in the destination array
     * @param length The number of elements that will be copied
     * @return true if the copy should proceed in the forward direction; false if it should proceed
     *         in the reverse direction
     */
    public static <TARRAY> boolean canCopyForward(TARRAY srcArray, int srcOffset, TARRAY destArray,
        int destOffset, int length) {
        return srcArray != destArray || // arrays different
            srcOffset + length <= destOffset || // case 1
            srcOffset >= destOffset; // cases 3, 4, 5
    }

    public static String dumpChunk(Chunk<? extends Any> chunk) {
        switch (chunk.getChunkType()) {
            case Boolean:
                return dumpChunk(chunk.asBooleanChunk());
            case Char:
                return dumpChunk(chunk.asCharChunk());
            case Byte:
                return dumpChunk(chunk.asByteChunk());
            case Short:
                return dumpChunk(chunk.asShortChunk());
            case Int:
                return dumpChunk(chunk.asIntChunk());
            case Long:
                return dumpChunk(chunk.asLongChunk());
            case Float:
                return dumpChunk(chunk.asFloatChunk());
            case Double:
                return dumpChunk(chunk.asDoubleChunk());
            case Object:
                return dumpChunk(chunk.asObjectChunk());
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static String dumpChunk(CharChunk<? extends Any> chunk) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (ii % 20 == 0) {
                if (ii > 0) {
                    builder.append("\n");
                }
                // noinspection UnnecessaryBoxing
                builder.append(String.format("%04d", Integer.valueOf(ii)));
            }
            final char charValue = chunk.get(ii);
            // noinspection UnnecessaryBoxing
            builder.append(" '").append(charValue).append("' ")
                .append(String.format("%6d", Integer.valueOf(charValue)));
        }
        return builder.append("\n").toString();
    }

    public static String dumpChunk(ByteChunk<? extends Any> chunk) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (ii % 20 == 0) {
                if (ii > 0) {
                    builder.append("\n");
                }
                // noinspection UnnecessaryBoxing
                builder.append(String.format("%04d", Integer.valueOf(ii)));
            }
            final byte byteValue = chunk.get(ii);
            // noinspection UnnecessaryBoxing
            builder.append(String.format(" %02x", Byte.valueOf(byteValue))).append(" ");
        }
        return builder.append("\n").toString();
    }

    public static String dumpChunk(ShortChunk<? extends Any> chunk) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (ii % 20 == 0) {
                if (ii > 0) {
                    builder.append("\n");
                }
                // noinspection UnnecessaryBoxing
                builder.append(String.format("%04d", Integer.valueOf(ii)));
            }
            final short shortValue = chunk.get(ii);
            // noinspection UnnecessaryBoxing
            builder.append(String.format(" %5d", Short.valueOf(shortValue))).append(" ");
        }
        return builder.append("\n").toString();
    }

    public static String dumpChunk(IntChunk<? extends Any> chunk) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (ii % 20 == 0) {
                if (ii > 0) {
                    builder.append("\n");
                }
                // noinspection UnnecessaryBoxing
                builder.append(String.format("%04d", Integer.valueOf(ii)));
            }
            final int intValue = chunk.get(ii);
            // noinspection UnnecessaryBoxing
            builder.append(String.format(" %10d", Integer.valueOf(intValue))).append(" ");
        }
        return builder.append("\n").toString();
    }

    public static String dumpChunk(LongChunk<? extends Any> chunk) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (ii % 20 == 0) {
                if (ii > 0) {
                    builder.append("\n");
                }
                // noinspection UnnecessaryBoxing
                builder.append(String.format("%04d", Integer.valueOf(ii)));
            }
            final long longValue = chunk.get(ii);
            // noinspection UnnecessaryBoxing
            builder.append(String.format(" %10d", Long.valueOf(longValue))).append(" ");
        }
        return builder.append("\n").toString();
    }

    public static String dumpChunk(FloatChunk<? extends Any> chunk) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (ii % 20 == 0) {
                if (ii > 0) {
                    builder.append("\n");
                }
                // noinspection UnnecessaryBoxing
                builder.append(String.format("%04d", Integer.valueOf(ii)));
            }
            final float floatValue = chunk.get(ii);
            // noinspection UnnecessaryBoxing
            builder.append(String.format(" %10f", Float.valueOf(floatValue))).append(" ");
        }
        return builder.append("\n").toString();
    }

    public static String dumpChunk(DoubleChunk<? extends Any> chunk) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (ii % 20 == 0) {
                if (ii > 0) {
                    builder.append("\n");
                }
                // noinspection UnnecessaryBoxing
                builder.append(String.format("%04d", Integer.valueOf(ii)));
            }
            final double doubleValue = chunk.get(ii);
            // noinspection UnnecessaryBoxing
            builder.append(String.format(" %10f", Double.valueOf(doubleValue))).append(" ");
        }
        return builder.append("\n").toString();
    }

    public static String dumpChunk(ObjectChunk<?, ? extends Any> chunk) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (ii % 10 == 0) {
                if (ii > 0) {
                    builder.append("\n");
                }
                // noinspection UnnecessaryBoxing
                builder.append(String.format("%04d", Integer.valueOf(ii)));
            }
            final Object value = chunk.get(ii);
            builder.append(String.format(" %20s", value)).append(" ");
        }
        return builder.append("\n").toString();
    }

    public static boolean contains(CharChunk<? extends Any> chunk, char value) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (chunk.get(ii) == value) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(ByteChunk<? extends Any> chunk, byte value) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (chunk.get(ii) == value) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(ShortChunk<? extends Any> chunk, short value) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (chunk.get(ii) == value) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(IntChunk<? extends Any> chunk, int value) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (chunk.get(ii) == value) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(LongChunk<? extends Any> chunk, long value) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (chunk.get(ii) == value) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(FloatChunk<? extends Any> chunk, float value) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (chunk.get(ii) == value) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(DoubleChunk<? extends Any> chunk, double value) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (chunk.get(ii) == value) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(ObjectChunk<?, ? extends Any> chunk, Object value) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            if (Objects.equals(chunk.get(ii), value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param src The source of the data.
     * @param srcAllKeys The source keys.
     * @param dest The destination of the data (dest != src).
     * @param destAllKeys The destination keys. It is ok for srcAllKeys == destAllKeys.
     * @param usePrev Should we read previous values from src
     */
    public static void copyData(ChunkSource.WithPrev<? extends Attributes.Values> src,
        OrderedKeys srcAllKeys, WritableSource dest,
        OrderedKeys destAllKeys, boolean usePrev) {
        if (src == dest) {
            throw new UnsupportedOperationException("This method isn't safe when src == dest");
        }
        if (srcAllKeys.size() != destAllKeys.size()) {
            final String msg =
                String.format("Expected srcAllKeys.size() == destAllKeys.size(), but got %d and %d",
                    srcAllKeys.size(), destAllKeys.size());
            throw new IllegalArgumentException(msg);
        }
        final int minSize = Math.min(srcAllKeys.intSize(), COPY_DATA_CHUNK_SIZE);
        if (minSize == 0) {
            return;
        }
        dest.ensureCapacity(destAllKeys.lastKey() + 1);
        try (final ChunkSource.GetContext srcContext = src.makeGetContext(minSize);
            final WritableChunkSink.FillFromContext destContext = dest.makeFillFromContext(minSize);
            final OrderedKeys.Iterator srcIter = srcAllKeys.getOrderedKeysIterator();
            final OrderedKeys.Iterator destIter = destAllKeys.getOrderedKeysIterator()) {
            while (srcIter.hasMore()) {
                Assert.assertion(destIter.hasMore(), "destIter.hasMore()");
                final OrderedKeys srcNextKeys = srcIter.getNextOrderedKeysWithLength(minSize);
                final OrderedKeys destNextKeys = destIter.getNextOrderedKeysWithLength(minSize);
                Assert.eq(srcNextKeys.size(), "srcNextKeys.size()", destNextKeys.size(),
                    "destNextKeys.size()");

                final Chunk<? extends Attributes.Values> chunk =
                    usePrev ? src.getPrevChunk(srcContext, srcNextKeys)
                        : src.getChunk(srcContext, srcNextKeys);
                dest.fillFromChunk(destContext, chunk, destNextKeys);
            }
        }
    }

    /**
     * Copy data from sources to destinations for the provided source and destination keys.
     *
     * Sources and destinations must not overlap.
     *
     * @param sources The sources of the data, parallel with destinations
     * @param srcAllKeys The source keys.
     * @param destinations The destinations, parallel with sources, of the data (dest != src).
     * @param destAllKeys The destination keys. It is ok for srcAllKeys == destAllKeys.
     * @param usePrev Should we read previous values from src
     */
    public static void copyData(ChunkSource.WithPrev<? extends Attributes.Values>[] sources,
        OrderedKeys srcAllKeys, WritableSource[] destinations,
        OrderedKeys destAllKeys, boolean usePrev) {
        if (srcAllKeys.size() != destAllKeys.size()) {
            final String msg =
                String.format("Expected srcAllKeys.size() == destAllKeys.size(), but got %d and %d",
                    srcAllKeys.size(), destAllKeys.size());
            throw new IllegalArgumentException(msg);
        }
        final int minSize = Math.min(srcAllKeys.intSize(), COPY_DATA_CHUNK_SIZE);
        if (minSize == 0) {
            return;
        }
        if (sources.length != destinations.length) {
            throw new IllegalArgumentException(
                "Expected sources and destinations to be parallel arrays: sources length="
                    + sources.length + ", destinations length=" + destinations.length);
        }

        final ChunkSource.GetContext[] sourceContexts = new ChunkSource.GetContext[sources.length];
        final WritableChunkSink.FillFromContext[] destContexts =
            new WritableChunkSink.FillFromContext[sources.length];

        try (final SharedContext sharedContext = SharedContext.makeSharedContext();
            final OrderedKeys.Iterator srcIter = srcAllKeys.getOrderedKeysIterator();
            final OrderedKeys.Iterator destIter = destAllKeys.getOrderedKeysIterator();
            final SafeCloseableArray<ChunkSource.GetContext> ignored =
                new SafeCloseableArray<>(sourceContexts);
            final SafeCloseableArray<WritableChunkSink.FillFromContext> ignored2 =
                new SafeCloseableArray<>(destContexts)) {

            for (int ss = 0; ss < sources.length; ++ss) {
                for (int dd = 0; dd < destinations.length; ++dd) {
                    if (sources[ss] == destinations[dd]) {
                        throw new IllegalArgumentException("Source must not equal destination!");
                    }
                }
                destinations[ss].ensureCapacity(destAllKeys.lastKey() + 1);
                sourceContexts[ss] = sources[ss].makeGetContext(minSize, sharedContext);
                destContexts[ss] = destinations[ss].makeFillFromContext(minSize);
            }

            while (srcIter.hasMore()) {
                Assert.assertion(destIter.hasMore(), "destIter.hasMore()");
                final OrderedKeys srcNextKeys = srcIter.getNextOrderedKeysWithLength(minSize);
                final OrderedKeys destNextKeys = destIter.getNextOrderedKeysWithLength(minSize);
                Assert.eq(srcNextKeys.size(), "srcNextKeys.size()", destNextKeys.size(),
                    "destNextKeys.size()");

                sharedContext.reset();
                for (int cc = 0; cc < sources.length; ++cc) {
                    final Chunk<? extends Attributes.Values> chunk =
                        usePrev ? sources[cc].getPrevChunk(sourceContexts[cc], srcNextKeys)
                            : sources[cc].getChunk(sourceContexts[cc], srcNextKeys);
                    destinations[cc].fillFromChunk(destContexts[cc], chunk, destNextKeys);
                }
            }
        }
    }

    public static <T extends Attributes.Values> void fillWithNullValue(WritableChunkSink<T> dest,
        OrderedKeys allKeys) {
        final int minSize = Math.min(allKeys.intSize(), COPY_DATA_CHUNK_SIZE);
        if (minSize == 0) {
            return;
        }
        try (
            final WritableChunkSink.FillFromContext destContext = dest.makeFillFromContext(minSize);
            final WritableChunk<T> chunk = dest.getChunkType().makeWritableChunk(minSize);
            final OrderedKeys.Iterator iter = allKeys.getOrderedKeysIterator()) {
            chunk.fillWithNullValue(0, minSize);
            while (iter.hasMore()) {
                try (final OrderedKeys nextKeys =
                    iter.getNextOrderedKeysWithLength(COPY_DATA_CHUNK_SIZE)) {
                    dest.fillFromChunk(destContext, chunk, nextKeys);
                }
            }
        }
    }

    /**
     * Make a chunk of integers in order.
     *
     * @param chunkSize the size of the chunk to make
     *
     * @return a chunk of integers from 0 to chunkSize - 1
     */
    public static <T extends Attributes.Any> WritableIntChunk<T> makeInOrderIntChunk(
        int chunkSize) {
        final WritableIntChunk<T> inOrderChunk = WritableIntChunk.makeWritableChunk(chunkSize);
        fillInOrder(inOrderChunk);
        return inOrderChunk;
    }

    /**
     * Fill inOrderChunk with consecutive integers from 0..size() - 1.
     *
     * @param inOrderChunk the chunk to fill
     */
    public static <T extends Any> void fillInOrder(WritableIntChunk<T> inOrderChunk) {
        for (int ii = 0; ii < inOrderChunk.size(); ++ii) {
            inOrderChunk.set(ii, ii);
        }
    }

    /**
     * Fill inOrderChunk with consecutive integers from 0..size() - 1.
     *
     * @param inOrderChunk the chunk to fill
     */
    public static <T extends Any> void fillInOrder(WritableLongChunk<T> inOrderChunk) {
        for (int ii = 0; ii < inOrderChunk.size(); ++ii) {
            inOrderChunk.set(ii, ii);
        }
    }
}
