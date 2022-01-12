package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

/**
 * The parser for the char type.
 */
public final class CharParser implements Parser<char[]> {
    public static final CharParser INSTANCE = new CharParser();

    private CharParser() {}

    @NotNull
    @Override
    public ParserContext<char[]> makeParserContext(final GlobalContext gctx, final int chunkSize) {
        final Sink<char[]> sink = gctx.sinkFactory.forChar();
        return new ParserContext<>(sink, null, new char[chunkSize]);
    }

    @Override
    public long tryParse(final GlobalContext gctx, final ParserContext<char[]> pctx, IteratorHolder ih,
            final long begin, final long end, final boolean appending) throws CsvReaderException {
        final MutableInt intHolder = new MutableInt();
        final Tokenizer t = gctx.tokenizer;
        final boolean[] nulls = gctx.nullChunk();

        final Sink<char[]> sink = pctx.sink();
        final Character reservedValue = gctx.sinkFactory.reservedChar();
        final char[] values = pctx.valueChunk();

        if (!gctx.isNullOrWidthOneSoFar) {
            return begin;
        }

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
            if (!t.tryParseBMPChar(ih.bs(), intHolder)) {
                gctx.isNullOrWidthOneSoFar = false;
                break;
            }
            final char value = (char) intHolder.intValue();
            if (reservedValue != null && value == reservedValue) {
                // If a sentinel null value is defined, it cannot be present in the input.
                break;
            }
            values[chunkIndex] = value;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
