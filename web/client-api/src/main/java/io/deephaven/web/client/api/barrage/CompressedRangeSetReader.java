//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CompressedRangeSetReader {

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

    private long pending = -1;
    private long offset = 0;
    private final List<Range> sortedRanges = new ArrayList<>();

    public static ByteBuffer writeRange(final RangeSet s) {
        long offset = 0;
        ByteBuffer payload = ByteBuffer.allocate(s.rangeCount() * 2 * (8 + 1) + 1);// max size it would need to be
        payload.order(ByteOrder.LITTLE_ENDIAN);
        ShortBuffer shorts = ShortBuffer.allocate(s.rangeCount() * 2);// assuming that every range will need 2 shorts
        for (Iterator<Range> it = s.rangeIterator(); it.hasNext();) {
            Range r = it.next();
            if (r.getLast() == r.getFirst()) {
                offset = appendWithDeltaOffset(payload, shorts, offset, r.getFirst(), false);
            } else {
                offset = appendWithDeltaOffset(payload, shorts, offset, r.getFirst(), false);
                offset = appendWithDeltaOffset(payload, shorts, offset, r.getLast(), true);
            }
        }
        flushShorts(payload, shorts);

        payload.put(END);

        payload.flip();
        ByteBuffer sliced = payload.slice();
        sliced.order(ByteOrder.LITTLE_ENDIAN);// this is required in JVM code, but apparently not in GWT emulation
        return sliced;
    }

    private static long appendWithDeltaOffset(final ByteBuffer payload, final ShortBuffer shorts,
            final long offset, final long value, final boolean negate) {
        final long delta = value - offset;
        if (delta >= Short.MAX_VALUE) {
            flushShorts(payload, shorts);
            writeValue(payload, OFFSET, negate ? -delta : delta);
            return value;
        }
        if (negate) {
            shorts.put((short) -delta);
        } else {
            shorts.put((short) delta);
        }
        return value;
    }

    private static void flushShorts(final ByteBuffer payload, final ShortBuffer shorts) {
        final int size = shorts.position();
        int writtenCount = 0;
        int consecutiveTrailingBytes = 0;
        for (int nextShortIndex = 0; nextShortIndex < size; ++nextShortIndex) {
            final short nextShort = shorts.get(nextShortIndex);
            if (nextShort <= Byte.MAX_VALUE && nextShort >= Byte.MIN_VALUE) {
                // nextShort can fit into a byte
                ++consecutiveTrailingBytes;
                continue;
            }
            // nextShort doesn't fit into a byte, so we've found the end of a (possibly-empty) sequence of bytes
            if (consecutiveTrailingBytes >= 4) { // See ExternalizableRowSetUtils.shouldWriteBytes for explanation
                // Write a possibly-empty prefix of shorts, followed by the consecutive bytes we found. Note that we're
                // not writing the short that triggered the end of the byte sequence; it will join the next sequence.
                final int shortCount = nextShortIndex - writtenCount - consecutiveTrailingBytes;
                writeShortsThenBytes(payload, shorts, writtenCount, shortCount, consecutiveTrailingBytes);
                writtenCount = nextShortIndex;
            }
            // Now we have at least one short, and no trailing bytes
            consecutiveTrailingBytes = 0;
        }

        // Write the remaining possibly-empty sequence of shorts, followed by any trailing consecutive bytes
        final int shortCount = size - writtenCount - consecutiveTrailingBytes;
        writeShortsThenBytes(payload, shorts, writtenCount, shortCount, consecutiveTrailingBytes);
        shorts.position(0);
    }

    private static void writeShortsThenBytes(final ByteBuffer payload, final ShortBuffer shorts,
            int index, final int shortCount, final int byteCount) {
        if (shortCount == 1) {
            writeValue(payload, OFFSET, shorts.get(index++));
        } else if (shortCount > 1) {
            writeValue(payload, SHORT_ARRAY, shortCount);
            final int shortLimit = index + shortCount;
            do {
                payload.putShort(shorts.get(index++));
            } while (index < shortLimit);
        }
        if (byteCount == 1) {
            writeValue(payload, OFFSET, shorts.get(index)); // Note, no increment, to avoid unused assignment
        } else if (byteCount > 1) {
            writeValue(payload, BYTE_ARRAY, byteCount);
            final int byteLimit = index + byteCount;
            do {
                payload.put((byte) shorts.get(index++));
            } while (index < byteLimit);
        }
    }

    private static void writeValue(final ByteBuffer out, final byte command, final long value) {
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            out.put((byte) (command | LONG_VALUE));
            out.putLong(value);
        } else if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            out.put((byte) (command | INT_VALUE));
            out.putInt((int) value);
        } else if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            out.put((byte) (command | SHORT_VALUE));
            out.putShort((short) value);
        } else {
            out.put((byte) (command | BYTE_VALUE));
            out.put((byte) value);
        }
    }

    public RangeSet read(ByteBuffer data) {
        do {
            long actualValue;
            final int command = data.get();
            switch (command & CMD_MASK) {
                case OFFSET:
                    final long value = readValue(data, command);
                    actualValue = offset + (value < 0 ? -value : value);
                    append(value < 0 ? -actualValue : actualValue);
                    offset = actualValue;
                    break;
                case SHORT_ARRAY:
                    final int shortCount = (int) readValue(data, command);
                    for (int ii = 0; ii < shortCount; ++ii) {
                        final short shortValue = data.getShort();
                        actualValue = offset + (shortValue < 0 ? -shortValue : shortValue);
                        append(shortValue < 0 ? -actualValue : actualValue);
                        offset = actualValue;
                    }
                    break;
                case BYTE_ARRAY:
                    final int byteCount = (int) readValue(data, command);
                    for (int ii = 0; ii < byteCount; ++ii) {
                        final byte byteValue = data.get();
                        actualValue = offset + (byteValue < 0 ? -byteValue : byteValue);
                        append(byteValue < 0 ? -actualValue : actualValue);
                        offset = actualValue;
                    }
                    break;
                case END:
                    if (pending >= 0) {
                        append(pending);
                    }
                    return RangeSet.fromSortedRanges(sortedRanges);
                default:
                    throw new IllegalStateException("Bad command: " + command + " at position " + data.position());
            }
        } while (true);

    }

    private void append(long value) {
        if (pending == -1) {
            pending = value;
        } else if (value < 0) {
            sortedRanges.add(new Range(pending, -value));
            pending = -1;
        } else {
            sortedRanges.add(new Range(pending, pending));
            pending = value;
        }
    }

    private static long readValue(final ByteBuffer in, final int command) {
        final long value;
        switch (command & VALUE_MASK) {
            case LONG_VALUE:
                value = in.getLong();
                break;
            case INT_VALUE:
                value = in.getInt();
                break;
            case SHORT_VALUE:
                value = in.getShort();
                break;
            case BYTE_VALUE:
                value = in.get();
                break;
            default:
                throw new IllegalStateException("Bad command: " + command);
        }
        return value;
    }
}
