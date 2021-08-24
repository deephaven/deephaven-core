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


    private long pending = -1;
    private long offset = 0;
    private List<Range> sortedRanges = new ArrayList<>();


    public static ByteBuffer writeRange(RangeSet s) {
        long offset = 0;
        ByteBuffer payload = ByteBuffer.allocate(s.rangeCount() * 2 * (8 + 1) + 1);// max size it
                                                                                   // would need to
                                                                                   // be
        payload.order(ByteOrder.LITTLE_ENDIAN);
        ShortBuffer shorts = ShortBuffer.allocate(s.rangeCount() * 2);// assuming that every range
                                                                      // will need 2 shorts
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
        sliced.order(ByteOrder.LITTLE_ENDIAN);// this is required in JVM code, but apparently not in
                                              // GWT emulation
        return sliced;
    }

    private static long appendWithDeltaOffset(ByteBuffer payload, ShortBuffer shorts, long offset,
        long value, boolean negate) {
        if (value >= offset + Short.MAX_VALUE) {
            flushShorts(payload, shorts);

            long newValue = value - offset;
            writeValue(payload, OFFSET, negate ? -newValue : newValue);
        } else if (negate) {
            shorts.put((short) -(value - offset));
        } else {
            shorts.put((short) (value - offset));
        }
        return value;
    }

    private static void flushShorts(ByteBuffer payload, ShortBuffer shorts) {
        for (int offset = 0; offset < shorts.position();) {
            int byteCount = 0;
            while (offset + byteCount < shorts.position()
                && (shorts.get(offset + byteCount) < Byte.MAX_VALUE
                    && shorts.get(offset + byteCount) > Byte.MIN_VALUE)) {
                byteCount++;
            }
            if (byteCount > 3 || byteCount + offset == shorts.position()) {
                if (byteCount == 1) {
                    writeValue(payload, OFFSET, shorts.get(offset));
                } else {
                    writeValue(payload, BYTE_ARRAY, byteCount);
                    for (int ii = offset; ii < offset + byteCount; ++ii) {
                        payload.put((byte) shorts.get(ii));
                    }
                }

                offset += byteCount;
            } else {
                int shortCount = byteCount;
                int consecutiveBytes = 0;
                while (shortCount + consecutiveBytes + offset < shorts.position()) {
                    final short shortValue = shorts.get(offset + shortCount + consecutiveBytes);
                    final boolean requiresShort =
                        (shortValue >= Byte.MAX_VALUE || shortValue <= Byte.MIN_VALUE);
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
                    && (offset + shortCount + consecutiveBytes == shorts.position())) {
                    shortCount += consecutiveBytes;
                }
                if (shortCount >= 2) {
                    writeValue(payload, SHORT_ARRAY, shortCount);
                    for (int ii = offset; ii < offset + shortCount; ++ii) {
                        payload.putShort(shorts.get(ii));
                    }
                } else if (shortCount == 1) {
                    writeValue(payload, OFFSET, shorts.get(offset));
                }
                offset += shortCount;
            }
        }
        shorts.position(0);
    }

    private static void writeValue(ByteBuffer out, byte command, long value) {
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
                    return RangeSet.fromSortedRanges(sortedRanges.toArray(new Range[0]));
                default:
                    throw new IllegalStateException(
                        "Bad command: " + command + " at position " + data.position());
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
