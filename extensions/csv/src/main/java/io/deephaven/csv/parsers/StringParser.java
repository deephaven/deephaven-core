package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.util.CsvReaderException;
import org.jetbrains.annotations.NotNull;

/**
 * The parser for the String type.
 */
public final class StringParser implements Parser<String[]> {
    public static final StringParser INSTANCE = new StringParser();

    private StringParser() {}

    @NotNull
    @Override
    public ParserContext<String[]> makeParserContext(final GlobalContext gctx, final int chunkSize) {
        final Sink<String[]> sink = gctx.sinkFactory.forString();
        return new ParserContext<>(sink, null, new String[chunkSize]);
    }

    @Override
    public long tryParse(final GlobalContext gctx, final ParserContext<String[]> pctx, IteratorHolder ih,
            final long begin, final long end, final boolean appending) throws CsvReaderException {
        final boolean[] nulls = gctx.nullChunk();

        final Sink<String[]> sink = pctx.sink();
        final String reservedValue = gctx.sinkFactory.reservedString();
        final String[] values = pctx.valueChunk();

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
            final String value = ih.bs().toString();
            if (value.equals(reservedValue)) {
                // If a reserved value is defined, it must not be present in the input.
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
