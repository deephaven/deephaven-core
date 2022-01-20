package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.jetbrains.annotations.NotNull;

/**
 * The parser for the double type.
 */
public final class DoubleParser implements Parser<double[]> {
    public static final DoubleParser INSTANCE = new DoubleParser();

    private DoubleParser() {}

    @NotNull
    @Override
    public ParserContext<double[]> makeParserContext(final GlobalContext gctx, final int chunkSize) {
        final Sink<double[]> sink = gctx.sinkFactory.forDouble();
        return new ParserContext<>(sink, null, new double[chunkSize]);
    }

    @Override
    public long tryParse(final GlobalContext gctx, final ParserContext<double[]> pctx, IteratorHolder ih,
            final long begin, final long end, final boolean appending) throws CsvReaderException {
        final MutableDouble doubleHolder = new MutableDouble();
        final Tokenizer t = gctx.tokenizer;
        final boolean[] nulls = gctx.nullChunk();

        final Sink<double[]> sink = pctx.sink();
        final Double reservedValue = gctx.sinkFactory.reservedDouble();
        final double[] values = pctx.valueChunk();

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
            if (!t.tryParseDouble(ih.bs(), doubleHolder)) {
                break;
            }
            final double value = doubleHolder.doubleValue();
            if (reservedValue != null && value == reservedValue) {
                // If a reserved value is defined, it must not be present in the input.
                break;
            }
            if (ih.bs().size() > 1) {
                gctx.isNullOrWidthOneSoFar = false;
            }
            values[chunkIndex] = value;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
