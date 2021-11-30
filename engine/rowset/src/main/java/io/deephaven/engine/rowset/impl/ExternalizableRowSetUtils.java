package io.deephaven.engine.rowset.impl;

import gnu.trove.list.array.TShortArrayList;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * Utility methods used for writing/reading {@link RowSet RowSets}.
 */
public class ExternalizableRowSetUtils {

    private static final byte SHORT_VALUE = 1;
    private static final byte INT_VALUE = 2;
    private static final byte LONG_VALUE = 3;
    private static final byte BYTE_VALUE = 4;

    private static final byte VALUE_MASK = 7;

    private static final byte OFFSET = 8;
    private static final byte SHORT_ARRAY = 16;
    private static final byte BYTE_ARRAY = 24;
    private static final byte END = 32;

    private static final byte CMD_MASK = 0x78;

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
        if (value >= offset + Short.MAX_VALUE) {
            flushShorts(out, shorts);

            final long newValue = value - offset;
            writeValue(out, OFFSET, negate ? -newValue : newValue);
            return value;
        }
        if (negate) {
            shorts.add((short) -(value - offset));
        } else {
            shorts.add((short) (value - offset));
        }
        return value;
    }


    private static void flushShorts(@NotNull final DataOutput out, @NotNull final TShortArrayList shorts)
            throws IOException {
        for (int offset = 0; offset < shorts.size();) {
            int byteCount = 0;
            while (offset + byteCount < shorts.size() && (shorts.getQuick(offset + byteCount) < Byte.MAX_VALUE
                    && shorts.getQuick(offset + byteCount) > Byte.MIN_VALUE)) {
                byteCount++;
            }
            if (byteCount > 3 || byteCount + offset == shorts.size()) {
                if (byteCount == 1) {
                    writeValue(out, OFFSET, shorts.getQuick(offset));
                } else {
                    writeValue(out, BYTE_ARRAY, byteCount);
                    for (int ii = offset; ii < offset + byteCount; ++ii) {
                        out.writeByte(shorts.getQuick(ii));
                    }
                }

                offset += byteCount;
            } else {
                int shortCount = byteCount;
                int consecutiveBytes = 0;
                while (shortCount + consecutiveBytes + offset < shorts.size()) {
                    final short shortValue = shorts.getQuick(offset + shortCount + consecutiveBytes);
                    final boolean requiresShort = (shortValue >= Byte.MAX_VALUE || shortValue <= Byte.MIN_VALUE);
                    if (!requiresShort) {
                        consecutiveBytes++;
                    } else {
                        consecutiveBytes = 0;
                        shortCount += consecutiveBytes;
                        shortCount++;
                    }
                    if (consecutiveBytes > 3) {
                        // switch to byte mode
                        break;
                    }
                }
                // if we have a small number of trailing bytes, tack them onto the end
                if (consecutiveBytes > 0 && consecutiveBytes <= 3
                        && (offset + shortCount + consecutiveBytes == shorts.size())) {
                    shortCount += consecutiveBytes;
                }
                if (shortCount >= 2) {
                    writeValue(out, SHORT_ARRAY, shortCount);
                    for (int ii = offset; ii < offset + shortCount; ++ii) {
                        out.writeShort(shorts.getQuick(ii));
                    }
                } else if (shortCount == 1) {
                    writeValue(out, OFFSET, shorts.getQuick(offset));
                }
                offset += shortCount;
            }
        }
        shorts.resetQuick();
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
            final long s = pending.longValue();
            if (s == -1) {
                pending.setValue(v);
            } else if (v < 0) {
                builder.appendRange(s, -v);
                pending.setValue(-1);
            } else {
                builder.appendKey(s);
                pending.setValue(v);
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

        if (pending.longValue() >= 0) {
            builder.appendKey(pending.longValue());
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
