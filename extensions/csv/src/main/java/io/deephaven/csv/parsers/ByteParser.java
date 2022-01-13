package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

/**
 * The parser for the byte type.
 */
public final class ByteParser implements Parser<byte[]> {
    public static final ByteParser INSTANCE = new ByteParser();

    private ByteParser() {}

    @NotNull
    @Override
    public ParserContext<byte[]> makeParserContext(final GlobalContext gctx, final int chunkSize) {
        final MutableObject<Source<byte[]>> sourceHolder = new MutableObject<>();
        final Sink<byte[]> sink = gctx.sinkFactory.forByte(sourceHolder);
        return new ParserContext<>(sink, sourceHolder.getValue(), new byte[chunkSize]);
    }

    @Override
    public long tryParse(final GlobalContext gctx, final ParserContext<byte[]> pctx, IteratorHolder ih,
            final long begin, final long end, final boolean appending) throws CsvReaderException {
        final MutableLong longHolder = new MutableLong();
        final Tokenizer t = gctx.tokenizer;
        final boolean[] nulls = gctx.nullChunk();

        final Sink<byte[]> sink = pctx.sink();
        final Byte reservedValue = gctx.sinkFactory.reservedByte();
        final byte[] values = pctx.valueChunk();

        long current = begin;
        int chunkIndex = 0;
        do {
            if (chunkIndex == values.length) {
                sink.write(values, nulls, current, current + chunkIndex, appending);
                current += chunkIndex;
                chunkIndex = 0;
            }
            if (current + chunkIndex == end) {
                break;
            }
            if (gctx.isNullCell(ih)) {
                nulls[chunkIndex++] = true;
                continue;
            }
            if (!t.tryParseLong(ih.bs(), longHolder)) {
                break;
            }
            final long value = longHolder.longValue();
            if (!RangeTests.isInRangeForByte(value)) {
                break;
            }
            if (reservedValue != null && value == reservedValue) {
                // If a reserved value is defined, it must not be present in the input.
                break;
            }
            if (ih.bs().size() != 1) {
                gctx.isNullOrWidthOneSoFar = false;
            }
            values[chunkIndex] = (byte) value;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
