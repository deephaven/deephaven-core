//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import gnu.trove.list.array.TShortArrayList;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * Utility methods used for writing/reading {@link RowSet RowSets}.
 */
public class ExternalizableRowSetUtils {

    // @formatter:off
    private static final byte SHORT_VALUE = 0b00000001;
    private static final byte INT_VALUE   = 0b00000010;
    private static final byte LONG_VALUE  = 0b00000011;
    private static final byte BYTE_VALUE  = 0b00000100;

    private static final byte VALUE_MASK  = 0b00000111;

    private static final byte OFFSET      = 0b00001000;
    private static final byte SHORT_ARRAY = 0b00010000;
    private static final byte BYTE_ARRAY  = 0b00011000;
    private static final byte END         = 0b00100000;

    private static final byte CMD_MASK    = 0b00111000;
    // @formatter:on

    /**
     * Write a {@link RowSet} to {@code out}.
     *
     * @param out The destination
     * @param rowSet The RowSet
     */
    public static void writeExternalCompressedDeltas(@NotNull final DataOutput out, @NotNull final RowSet rowSet)
            throws IOException {
        long offset = 0;
        final TShortArrayList shorts = new TShortArrayList();

        for (final RowSet.RangeIterator it = rowSet.rangeIterator(); it.hasNext();) {
            it.next();
            if (it.currentRangeEnd() == it.currentRangeStart()) {
                offset = appendWithOffsetDelta(out, shorts, offset, it.currentRangeStart(), false);
            } else {
                offset = appendWithOffsetDelta(out, shorts, offset, it.currentRangeStart(), false);
                offset = appendWithOffsetDelta(out, shorts, offset, it.currentRangeEnd(), true);
            }
        }

        flushShorts(out, shorts);

        out.writeByte(END);
    }

    private static long appendWithOffsetDelta(@NotNull final DataOutput out, @NotNull final TShortArrayList shorts,
            final long offset, final long value, final boolean negate) throws IOException {
        final long delta = value - offset;
        if (delta >= Short.MAX_VALUE) {
            flushShorts(out, shorts);
            writeValue(out, OFFSET, negate ? -delta : delta);
            return value;
        }
        if (negate) {
            shorts.add((short) -delta);
        } else {
            shorts.add((short) delta);
        }
        return value;
    }

    private static void flushShorts(@NotNull final DataOutput out, @NotNull final TShortArrayList shorts)
            throws IOException {
        final int size = shorts.size();
        int writtenCount = 0;
        int consecutiveTrailingBytes = 0;
        for (int nextShortIndex = 0; nextShortIndex < size; ++nextShortIndex) {
            final short nextShort = shorts.getQuick(nextShortIndex);
            if (nextShort <= Byte.MAX_VALUE && nextShort >= Byte.MIN_VALUE) {
                // nextShort can fit into a byte
                ++consecutiveTrailingBytes;
                continue;
            }
            // nextShort doesn't fit into a byte, so we've found the end of a (possibly-empty) sequence of bytes
            if (shouldWriteBytes(consecutiveTrailingBytes)) {
                // Write a possibly-empty prefix of shorts, followed by the consecutive bytes we found. Note that we're
                // not writing the short that triggered the end of the byte sequence; it will join the next sequence.
                final int shortCount = nextShortIndex - writtenCount - consecutiveTrailingBytes;
                writeShortsThenBytes(out, shorts, writtenCount, shortCount, consecutiveTrailingBytes);
                writtenCount = nextShortIndex;
            }
            // Now we have at least one short, and no trailing bytes
            consecutiveTrailingBytes = 0;
        }

        // Write the remaining possibly-empty sequence of shorts, followed by any trailing consecutive bytes
        final int shortCount = size - writtenCount - consecutiveTrailingBytes;
        writeShortsThenBytes(out, shorts, writtenCount, shortCount, consecutiveTrailingBytes);
        shorts.resetQuick();
    }

    private static boolean shouldWriteBytes(final int byteCount) {
        /*
         * @formatter:off
         * =============================================================================================================
         * Why do we use 4 as the size cutoff for writing a byte sequence?
         * =============================================================================================================
         *
         * First, we consider the direct cost savings by writing bytes instead of appending them to a sequence of
         *  shorts:
         *
         * -------------------------------------------------------------------------------------------------------------
         * byteCount | output bytes                                          | saved output bytes
         * -------------------------------------------------------------------------------------------------------------
         *         1 | 2 (1 command byte, 1 value byte)                      | 0
         *         2 | 4 (1 command byte, 1 length byte, 2 value bytes)      | 0
         *     N > 3 | 2 + N (1 command byte, 1 length byte, N values bytes) | 1 + N - 3
         *     N = 3 | 2 + 3 (1 command byte, 1 length byte, 3 value bytes)  | 1
         *     N = 4 | 2 + 4 (1 command byte, 1 length byte, 4 value bytes)  | 2
         * -------------------------------------------------------------------------------------------------------------
         *
         * This would seem to argue for splitting at 3 bytes, but we need to consider the impact of splitting a
         * sequence of shorts unless our byte sequence is at the beginning or end of the overall sequence. We need only
         * "pay" for one side, since that's the "extra" cost imposed by writing our byte sequence as bytes:
         *
         * -------------------------------------------------------------------------------------------------------------
         *        shortCount | extra output bytes (excludes value bytes)
         * -------------------------------------------------------------------------------------------------------------
         *                 1 | 1 (1 command byte)
         *             2-127 | 2 (1 command byte, 1 length byte)
         *         128-32767 | 3 (1 command byte, 2 length bytes)
         *  32768-2147483647 | 5 (1 command byte, 4 length bytes)
         *       2147483647+ | 9 (1 command byte, 8 length bytes)
         * -------------------------------------------------------------------------------------------------------------
         *
         * We could build a complex checker that perfectly minimizes the total number of bytes written, but given the
         * number of shorts we're amortizing over at 128+, I'm inclined to keep this simple and just split at 4 bytes.
         *
         * =============================================================================================================
         * At the end of our overall sequence it's never worse to write bytes if we have them, so we always write bytes.
         * =============================================================================================================
         * @formatter:on
         */
        return byteCount >= 4;
    }

    private static void writeShortsThenBytes(@NotNull final DataOutput out, @NotNull final TShortArrayList shorts,
            int index, final int shortCount, final int byteCount) throws IOException {
        if (shortCount == 1) {
            writeValue(out, OFFSET, shorts.getQuick(index++));
        } else if (shortCount > 1) {
            writeValue(out, SHORT_ARRAY, shortCount);
            final int shortLimit = index + shortCount;
            do {
                out.writeShort(shorts.getQuick(index++));
            } while (index < shortLimit);
        }
        if (byteCount == 1) {
            writeValue(out, OFFSET, shorts.getQuick(index)); // Note, no increment, to avoid unused assignment
        } else if (byteCount > 1) {
            writeValue(out, BYTE_ARRAY, byteCount);
            final int byteLimit = index + byteCount;
            do {
                out.writeByte(shorts.getQuick(index++));
            } while (index < byteLimit);
        }
    }

    private static void writeValue(@NotNull final DataOutput out, final byte command, final long value)
            throws IOException {
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            out.writeByte(command | LONG_VALUE);
            out.writeLong(value);
        } else if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            out.writeByte(command | INT_VALUE);
            out.writeInt((int) value);
        } else if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            out.writeByte(command | SHORT_VALUE);
            out.writeShort((short) value);
        } else {
            out.writeByte(command | BYTE_VALUE);
            out.writeByte((byte) value);
        }
    }

    public static RowSet readExternalCompressedDelta(@NotNull final DataInput in) throws IOException {
        long offset = 0;
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        final MutableLong pending = new MutableLong(-1);
        final LongConsumer consume = v -> {
            final long s = pending.get();
            if (s == -1) {
                pending.set(v);
            } else if (v < 0) {
                builder.appendRange(s, -v);
                pending.set(-1);
            } else {
                builder.appendKey(s);
                pending.set(v);
            }
        };

        WHILE: do {
            long actualValue;
            final int command = in.readByte();
            switch (command & CMD_MASK) {
                case OFFSET:
                    final long value = readValue(in, command);
                    actualValue = offset + (value < 0 ? -value : value);
                    consume.accept(value < 0 ? -actualValue : actualValue);
                    offset = actualValue;
                    break;
                case SHORT_ARRAY:
                    final int shortCount = (int) readValue(in, command);
                    for (int ii = 0; ii < shortCount; ++ii) {
                        final short shortValue = in.readShort();
                        actualValue = offset + (shortValue < 0 ? -shortValue : shortValue);
                        consume.accept(shortValue < 0 ? -actualValue : actualValue);
                        offset = actualValue;
                    }
                    break;
                case BYTE_ARRAY:
                    final int byteCount = (int) readValue(in, command);
                    for (int ii = 0; ii < byteCount; ++ii) {
                        final byte byteValue = in.readByte();
                        actualValue = offset + (byteValue < 0 ? -byteValue : byteValue);
                        consume.accept(byteValue < 0 ? -actualValue : actualValue);
                        offset = actualValue;
                    }
                    break;
                case END:
                    break WHILE;
                default:
                    throw new IllegalStateException("Bad command: " + command);
            }
        } while (true);

        if (pending.get() >= 0) {
            builder.appendKey(pending.get());
        }

        return builder.build();
    }

    private static long readValue(@NotNull final DataInput in, final int command) throws IOException {
        final long value;
        switch (command & VALUE_MASK) {
            case LONG_VALUE:
                value = in.readLong();
                break;
            case INT_VALUE:
                value = in.readInt();
                break;
            case SHORT_VALUE:
                value = in.readShort();
                break;
            case BYTE_VALUE:
                value = in.readByte();
                break;
            default:
                throw new IllegalStateException("Bad command: " + command);
        }
        return value;
    }
}
