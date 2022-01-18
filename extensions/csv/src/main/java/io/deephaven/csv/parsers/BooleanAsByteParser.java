package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

/**
 * The parser for the boolean (as byte) type.
 */
public final class BooleanAsByteParser implements Parser<byte[]> {
    public static BooleanAsByteParser INSTANCE = new BooleanAsByteParser();

    private BooleanAsByteParser() {}

    @NotNull
    @Override
    public ParserContext<byte[]> makeParserContext(final GlobalContext gctx, final int chunkSize) {
        final Sink<byte[]> sink = gctx.sinkFactory.forBooleanAsByte();
        return new ParserContext<>(sink, null, new byte[chunkSize]);
    }

    @Override
    public long tryParse(final GlobalContext gctx, final ParserContext<byte[]> pctx, IteratorHolder ih,
            final long begin, final long end, final boolean appending) throws CsvReaderException {
        final MutableBoolean booleanHolder = new MutableBoolean();
        final Tokenizer t = gctx.tokenizer;
        final boolean[] nulls = gctx.nullChunk();

        final Sink<byte[]> sink = pctx.sink();
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
            if (!t.tryParseBoolean(ih.bs(), booleanHolder)) {
                break;
            }
            gctx.isNullOrWidthOneSoFar = false;
            values[chunkIndex] = booleanHolder.booleanValue() ? (byte) 1 : (byte) 0;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
