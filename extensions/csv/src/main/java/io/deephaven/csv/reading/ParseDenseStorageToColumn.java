package io.deephaven.csv.reading;

import io.deephaven.csv.parsers.*;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.IntConsumer;

/**
 * The job of this class is to take a column of cell text, as prepared by {@link ParseInputToDenseStorage}, do type
 * inference if appropriate, and parse the text into typed data.
 */
public final class ParseDenseStorageToColumn {
    /**
     * @param dsr A reader for the input.
     * @param dsrAlt A second reader for the same input (used to perform the second pass over the data, if type
     *        inference deems a second pass to be necessary).
     * @param parsers The set of parsers to try. If null, then {@link Parsers#DEFAULT} will be used.
     * @param nullValueLiteral If a cell text is equal to this value, it will be interpreted as the null value.
     *        Typically set to the empty string.
     * @param nullParser The Parser to use if parsers.size() > 1 but the column contains all null values. This is needed
     *        as a backstop because otherwise type inference would have no way to choose among the multiple parsers.
     * @param sinkFactory Factory that makes all of the Sinks of various types, used to consume the data we produce.
     * @return The {@link Sink}, provided by the caller's {@link SinkFactory}, that was selected to hold the column
     *         data.
     */
    public static Sink<?> doit(final DenseStorageReader dsr, final DenseStorageReader dsrAlt,
            List<Parser<?>> parsers, final Parser<?> nullParser,
            final Tokenizer.CustomTimeZoneParser customTimeZoneParser,
            final String nullValueLiteral,
            final SinkFactory sinkFactory) throws CsvReaderException {
        Set<Parser<?>> parserSet = new HashSet<>(Objects.requireNonNullElse(parsers, Parsers.DEFAULT));

        final Tokenizer tokenizer = new Tokenizer(customTimeZoneParser);
        final Parser.GlobalContext gctx = new Parser.GlobalContext(tokenizer, sinkFactory, nullValueLiteral);

        // Skip over leading null cells. There are three cases:
        // 1. There is a non-null cell (so the type inference process can begin)
        // 2. The column is full of all nulls
        // 3. The column is empty
        final IteratorHolder ih = new IteratorHolder(dsr);
        boolean columnIsEmpty = true;
        boolean columnIsAllNulls = true;
        while (ih.tryMoveNext()) {
            columnIsEmpty = false;
            if (!gctx.isNullCell(ih)) {
                columnIsAllNulls = false;
                break;
            }
        }

        if (columnIsAllNulls) {
            // We get here in cases 2 and 3: the column is all nulls, or the column is empty.
            final Parser<?> nullParserToUse = parserSet.size() == 1 ? parserSet.iterator().next() : nullParser;
            if (nullParserToUse == null) {
                throw new CsvReaderException(
                        "Column contains all null cells: can't infer type of column, and nullParser is not set.");
            }
            if (columnIsEmpty) {
                return emptyParse(nullParserToUse, gctx);
            }
            return onePhaseParse(nullParserToUse, gctx, dsrAlt);
        }

        final CategorizedParsers cats = CategorizedParsers.create(parserSet);

        if (cats.customParser != null) {
            return onePhaseParse(cats.customParser, gctx, dsrAlt);
        }

        // Numerics are special and they get their own fast path that uses Sources and Sinks rather than
        // reparsing the text input.
        final MutableDouble dummyDouble = new MutableDouble();
        if (!cats.numericParsers.isEmpty() && tokenizer.tryParseDouble(ih.bs(), dummyDouble)) {
            return parseNumerics(cats, gctx, ih, dsrAlt);
        }

        List<Parser<?>> universeByPrecedence = List.of(Parsers.CHAR, Parsers.STRING);
        final MutableBoolean dummyBoolean = new MutableBoolean();
        final MutableLong dummyLong = new MutableLong();
        if (cats.timestampParser != null && tokenizer.tryParseLong(ih.bs(), dummyLong)) {
            universeByPrecedence = List.of(cats.timestampParser, Parsers.CHAR, Parsers.STRING);
        } else if (cats.booleanParser != null && tokenizer.tryParseBoolean(ih.bs(), dummyBoolean)) {
            universeByPrecedence = List.of(Parsers.BOOLEAN, Parsers.STRING);
        } else if (cats.dateTimeParser != null && tokenizer.tryParseDateTime(ih.bs(), dummyLong)) {
            universeByPrecedence = List.of(Parsers.DATETIME, Parsers.STRING);
        }
        List<Parser<?>> parsersToUse = limitToSpecified(universeByPrecedence, parserSet);
        return parseFromList(parsersToUse, gctx, ih, dsrAlt);
    }

    @NotNull
    private static Sink<?> parseNumerics(
            CategorizedParsers cats, final Parser.GlobalContext gctx, final IteratorHolder ih,
            final DenseStorageReader dsrAlt)
            throws CsvReaderException {
        final List<ParserResultWrapper> wrappers = new ArrayList<>();
        for (Parser<?> parser : cats.numericParsers) {
            final ParserResultWrapper prw = parseNumericsHelper(parser, gctx, ih);
            wrappers.add(prw);
            if (ih.isExhausted()) {
                // Parsed everything with numerics!
                return unifyNumericResults(gctx, wrappers);
            }
        }

        return parseFromList(cats.charAndStringParsers, gctx, ih, dsrAlt);
    }

    @NotNull
    private static <TARRAY> ParserResultWrapper parseNumericsHelper(Parser<TARRAY> parser,
            final Parser.GlobalContext gctx, final IteratorHolder ih)
            throws CsvReaderException {
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        final long begin = ih.numConsumed() - 1;
        final long end = parser.tryParse(gctx, pctx, ih, begin, Long.MAX_VALUE, true);
        return new ParserResultWrapper(pctx, begin, end);
    }

    @NotNull
    private static Sink<?> parseFromList(final List<Parser<?>> parsers,
            final Parser.GlobalContext gctx, final IteratorHolder ih,
            final DenseStorageReader dsrAlt) throws CsvReaderException {
        if (parsers.isEmpty()) {
            throw new CsvReaderException("No available parsers.");
        }

        for (int ii = 0; ii < parsers.size() - 1; ++ii) {
            final Sink<?> result = tryTwoPhaseParse(parsers.get(ii), gctx, ih, dsrAlt);
            if (result != null) {
                return result;
            }
        }

        // The final parser in the set gets special (more efficient) handling because there's nothing to fall back to.
        return onePhaseParse(parsers.get(parsers.size() - 1), gctx, dsrAlt);
    }

    private static <TARRAY> Sink<TARRAY> tryTwoPhaseParse(final Parser<TARRAY> parser,
            final Parser.GlobalContext gctx, final IteratorHolder ih,
            final DenseStorageReader dsrAlt) throws CsvReaderException {
        final long phaseOneStart = ih.numConsumed() - 1;
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        parser.tryParse(gctx, pctx, ih, phaseOneStart, Long.MAX_VALUE, true);
        if (!ih.isExhausted()) {
            // This parser couldn't make it to the end but there are others remaining to try. Signal a failure to the
            // caller so that it can try the next one.
            return null;
        }
        if (phaseOneStart == 0) {
            // Reached end, and started at zero so everything was parsed and we are done.
            return pctx.sink();
        }

        final IteratorHolder ihAlt = new IteratorHolder(dsrAlt);
        ihAlt.tryMoveNext(); // Input is not empty, so we know this will succeed.
        final long end = parser.tryParse(gctx, pctx, ihAlt, 0, phaseOneStart, false);

        if (end == phaseOneStart) {
            return pctx.sink();
        }
        final String message =
                "Logic error: second parser phase failed on input. Parser was: " + parser.getClass().getCanonicalName();
        throw new RuntimeException(message);
    }

    @NotNull
    private static <TARRAY> Sink<TARRAY> onePhaseParse(final Parser<TARRAY> parser,
            final Parser.GlobalContext gctx,
            final DenseStorageReader dsrAlt) throws CsvReaderException {
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        final IteratorHolder ihAlt = new IteratorHolder(dsrAlt);
        ihAlt.tryMoveNext(); // Input is not empty, so we know this will succeed.
        parser.tryParse(gctx, pctx, ihAlt, 0, Long.MAX_VALUE, true);
        if (ihAlt.isExhausted()) {
            return pctx.sink();
        }
        final String message = "One phase parser failed on input. Parser was: " + parser.getClass().getCanonicalName();
        throw new CsvReaderException(message);
    }

    @NotNull
    private static <TARRAY> Sink<TARRAY> emptyParse(final Parser<TARRAY> parser,
            final Parser.GlobalContext gctx) throws CsvReaderException {
        // The parser won't do any "parsing" here, but it will create a Sink.
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        parser.tryParse(gctx, pctx, null, 0, 0, true); // Result ignored.
        return pctx.sink();
    }

    @NotNull
    private static Sink<?> unifyNumericResults(final Parser.GlobalContext gctx,
            final List<ParserResultWrapper> wrappers) {
        if (wrappers.isEmpty()) {
            throw new RuntimeException("Logic error: no parser results.");
        }
        final ParserResultWrapper dest = wrappers.get(wrappers.size() - 1);

        // BTW, there's an edge case where there's only one parser in the list. In that case first == dest,
        // but this code still does the right thing.
        final ParserResultWrapper first = wrappers.get(0);
        fillNulls(gctx, dest.pctx, 0, first.begin);

        long destBegin = first.begin;
        for (int ii = 0; ii < wrappers.size() - 1; ++ii) {
            final ParserResultWrapper curr = wrappers.get(ii);
            copy(gctx, curr.pctx, dest.pctx, curr.begin, curr.end, destBegin);
            destBegin += (curr.end - curr.begin);
        }
        return dest.pctx.sink();
    }

    private static <TARRAY, UARRAY> void copy(final Parser.GlobalContext gctx,
            final Parser.ParserContext<TARRAY> sourceCtx, final Parser.ParserContext<UARRAY> destCtx,
            final long srcBegin, final long srcEnd,
            final long destBegin) {
        TypeConverter.copy(sourceCtx.source(), destCtx.sink(),
                srcBegin, srcEnd, destBegin,
                sourceCtx.valueChunk(), destCtx.valueChunk(), gctx.nullChunk());
    }


    private static <TARRAY> void fillNulls(final Parser.GlobalContext gctx,
            final Parser.ParserContext<TARRAY> pctx,
            final long begin, final long end) {
        if (begin == end) {
            return;
        }
        final boolean[] nullBuffer = gctx.nullChunk();
        final Sink<TARRAY> destSink = pctx.sink();
        final TARRAY values = pctx.valueChunk();

        final int sizeToInit = Math.min(nullBuffer.length, Math.toIntExact(end - begin));
        Arrays.fill(nullBuffer, 0, sizeToInit, true);

        for (long current = begin; current != end;) { // no ++
            final long endToUse = Math.min(current + nullBuffer.length, end);
            // Don't care about the actual values, only the null flag values (which are all true).
            destSink.write(values, nullBuffer, current, endToUse, false);
            current = endToUse;
        }
    }

    private static <T> List<T> limitToSpecified(Collection<T> items, Set<T> limitTo) {
        final List<T> result = new ArrayList<>();
        for (T item : items) {
            if (limitTo.contains(item)) {
                result.add(item);
            }
        }
        return result;
    }

    private static class CategorizedParsers {
        public static CategorizedParsers create(final Collection<Parser<?>> parsers) throws CsvReaderException {
            Parser<?> booleanParser = null;
            final Set<Parser<?>> specifiedNumericParsers = new HashSet<>();
            // Subset of the above.
            final List<Parser<?>> specifiedFloatingPointParsers = new ArrayList<>();
            Parser<?> dateTimeParser = null;
            final Set<Parser<?>> specifiedCharAndStringParsers = new HashSet<>();
            final List<Parser<?>> specifiedTimeStampParsers = new ArrayList<>();
            final List<Parser<?>> specifiedCustomParsers = new ArrayList<>();
            for (Parser<?> p : parsers) {
                if (p == Parsers.BYTE || p == Parsers.SHORT || p == Parsers.INT || p == Parsers.LONG) {
                    specifiedNumericParsers.add(p);
                    continue;
                }

                if (p == Parsers.FLOAT_FAST || p == Parsers.FLOAT_STRICT || p == Parsers.DOUBLE) {
                    specifiedNumericParsers.add(p);
                    specifiedFloatingPointParsers.add(p);
                    continue;
                }

                if (p == Parsers.TIMESTAMP_SECONDS || p == Parsers.TIMESTAMP_MILLIS || p == Parsers.TIMESTAMP_MICROS
                        || p == Parsers.TIMESTAMP_NANOS) {
                    specifiedTimeStampParsers.add(p);
                    continue;
                }

                if (p == Parsers.CHAR || p == Parsers.STRING) {
                    specifiedCharAndStringParsers.add(p);
                    continue;
                }

                if (p == Parsers.BOOLEAN) {
                    booleanParser = p;
                    continue;
                }

                if (p == Parsers.DATETIME) {
                    dateTimeParser = p;
                    continue;
                }

                specifiedCustomParsers.add(p);
            }

            if (specifiedFloatingPointParsers.size() > 1) {
                throw new CsvReaderException("There is more than one floating point parser in the parser set.");
            }

            if (specifiedTimeStampParsers.size() > 1) {
                throw new CsvReaderException("There is more than one timestamp parser in the parser set.");
            }

            if (specifiedCustomParsers.size() > 1) {
                throw new CsvReaderException("There is more than one custom parser in the parser set.");
            }

            if (!specifiedCustomParsers.isEmpty() && parsers.size() != 1) {
                throw new CsvReaderException(
                        "When a custom parser is specified, it must be the only parser in the set.");
            }

            if (!specifiedNumericParsers.isEmpty() && !specifiedTimeStampParsers.isEmpty()) {
                throw new CsvReaderException("The parser set must not contain both numeric and timestamp parsers.");
            }

            final List<Parser<?>> allNumericParsersByPrecedence = List.of(
                    Parsers.BYTE, Parsers.SHORT, Parsers.INT, Parsers.LONG, Parsers.FLOAT_FAST, Parsers.FLOAT_STRICT,
                    Parsers.DOUBLE);
            final List<Parser<?>> allCharAndStringParsersByPrecedence = List.of(
                    Parsers.CHAR, Parsers.STRING);

            final List<Parser<?>> numericParsers =
                    limitToSpecified(allNumericParsersByPrecedence, specifiedNumericParsers);
            final List<Parser<?>> charAndStringParsers =
                    limitToSpecified(allCharAndStringParsersByPrecedence, specifiedCharAndStringParsers);
            final Parser<?> timestampParser =
                    specifiedTimeStampParsers.isEmpty() ? null : specifiedTimeStampParsers.get(0);
            final Parser<?> customParser = specifiedCustomParsers.isEmpty() ? null : specifiedCustomParsers.get(0);

            return new CategorizedParsers(booleanParser, numericParsers, dateTimeParser,
                    charAndStringParsers, timestampParser, customParser);
        }

        private final Parser<?> booleanParser;
        private final List<Parser<?>> numericParsers;
        private final Parser<?> dateTimeParser;
        private final List<Parser<?>> charAndStringParsers;
        private final Parser<?> timestampParser;
        private final Parser<?> customParser;

        private CategorizedParsers(Parser<?> booleanParser, List<Parser<?>> numericParsers, Parser<?> dateTimeParser,
                List<Parser<?>> charAndStringParsers, Parser<?> timestampParser, Parser<?> customParser) {
            this.booleanParser = booleanParser;
            this.numericParsers = numericParsers;
            this.dateTimeParser = dateTimeParser;
            this.charAndStringParsers = charAndStringParsers;
            this.timestampParser = timestampParser;
            this.customParser = customParser;
        }
    }

    private static class ParserResultWrapper {
        private final Parser.ParserContext<?> pctx;
        private final long begin;
        private final long end;

        public ParserResultWrapper(Parser.ParserContext<?> pctx, long begin, long end) {
            this.pctx = pctx;
            this.begin = begin;
            this.end = end;
        }
    }
}
