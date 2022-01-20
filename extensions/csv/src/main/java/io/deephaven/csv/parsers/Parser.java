package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The Parser interface to the CsvReader. This is implemented by all the built-in parsers {@link IntParser},
 * {@link DoubleParser}, etc, as well as user-defined custom parsers.
 * 
 * @param <TARRAY>
 */
public interface Parser<TARRAY> {
    int CHUNK_SIZE = 65536 * 4;

    /**
     * Make a context object for the parser. Sample implementation: <code><pre>
     * final MySink sink = new MySink();
     * return new ParserContext<>(sink, null, new MyType[chunkSize]);
     * </pre></code>
     *
     * <p>
     * Note that parsers other than {Byte,Short,Int,Long}Parser can leave the source field null, as in the above
     * example.
     * 
     * @param gctx The GlobalContext. Built-in parsers use this to access the SinkFactory so that they can make a Sink
     *        of the right type. Custom parsers will probably not need this.
     * @param chunkSize The size of the chunk to create.
     * @return The ParserContext.
     */
    @NotNull
    ParserContext<TARRAY> makeParserContext(final GlobalContext gctx, final int chunkSize);

    /**
     * Tries to parse the data pointed to by IteratorHolder 'ih' into a Sink. The method parses as many values as it
     * can. It stops when:
     * <ol>
     * <li>The range [{@code destBegin},{@code destEnd}) is full, or</li>
     * <li>The iterator {@code ih} is exhausted, or</li>
     * <li>The code encounters a source value that it is unable to parse.</li>
     * </ol>
     * 
     * @param gctx The {@link GlobalContext} holding various shared parameters for the parse. This will be shared among
     *        parsers of different types as the type inference process proceeds.
     * @param pctx The {@link ParserContext} for this specific parser. It will be the object created by the call to
     *        {Parser#makeContext}. If the caller calls {@link Parser#tryParse} multiple times (for example during
     *        two-phase parsing), it will pass the same {@link ParserContext} object each time.
     * @param ih An IteratorHolder pointing to the data. It is already pointing to the current element or the end (in
     *        other words, it has had {@link IteratorHolder#tryMoveNext} called on it at least once). The reason for
     *        this invariant is because other code (controlling logic and other parsers) have needed to peek at the
     *        current element before getting here in order to decide what to do.
     * @param begin The start of the range (inclusive) to write values to.
     * @param end The end of the range (exclusive) to write values to. This can also be a very large value like
     *        Long.MAX_VALUE if the caller does not know how many values there are.
     * @param appending Whether the parser is being called in a mode where it is appending to the end of the
     *        {@link Sink} or replacing previously-written pad values in the {@link Sink}. This value is simply passed
     *        on to {@link Sink#write} which may use it as a hint to slightly simplify its logic.
     * @return The end range (exclusive) of the values parsed. Returns {@code begin} if no values were parsed.
     */
    long tryParse(GlobalContext gctx, ParserContext<TARRAY> pctx, IteratorHolder ih,
            long begin, long end, boolean appending) throws CsvReaderException;

    class GlobalContext {
        /**
         * The Tokenizer is responsible for parsing entities like ints, doubles, supported DateTime formats, etc.
         */
        public final Tokenizer tokenizer;
        /**
         * Caller-specified interface for making all the various Sink&lt;TARRAY&gt; types.
         */
        public final SinkFactory sinkFactory;
        /**
         * Whether all the cells seen so far are the "null" indicator (usually the empty string), or are 1 character in
         * length. This is used when inferring char vs String.
         */
        public boolean isNullOrWidthOneSoFar;
        /**
         * If the null sentinel is not the empty string, then this field contains the UTF-8 encoded bytes of the null
         * sentinel string. Otherwise this field contains null.
         */
        private final byte[] nullSentinelBytes;
        /**
         * An "isNull" chunk
         */
        private final boolean[] nullChunk;

        public GlobalContext(final Tokenizer tokenizer, final SinkFactory sinkFactory, final String nullValueLiteral) {
            this.tokenizer = tokenizer;
            this.sinkFactory = sinkFactory;
            isNullOrWidthOneSoFar = true;

            // Process the nullValueLiteral into a byte array so the isNullCell test can run quickly.
            nullSentinelBytes = nullValueLiteral.getBytes(StandardCharsets.UTF_8);
            nullChunk = new boolean[CHUNK_SIZE];
        }

        /**
         * Determines whether the iterator's current text contains the null value literal. The notion of "null value
         * literal" is user-configurable on a per-column basis, but is typically the empty string.
         *
         * @return whether the iterator's current text contains the null cell.
         */
        public boolean isNullCell(final IteratorHolder ih) {
            // A possibly-needless optimization.
            if (nullSentinelBytes.length == 0) {
                return ih.bs().size() == 0;
            }
            return Arrays.equals(ih.bs().data(), ih.bs().begin(), ih.bs().end(),
                    nullSentinelBytes, 0, nullSentinelBytes.length);
        }

        public boolean[] nullChunk() {
            return nullChunk;
        }
    }

    class ParserContext<TARRAY> {
        private final Sink<TARRAY> sink;
        private final Source<TARRAY> source;
        private final TARRAY valueChunk;

        public ParserContext(Sink<TARRAY> sink, Source<TARRAY> source, TARRAY valueChunk) {
            this.sink = sink;
            this.source = source;
            this.valueChunk = valueChunk;
        }

        public Sink<TARRAY> sink() {
            return sink;
        }

        public Source<TARRAY> source() {
            return source;
        }

        public TARRAY valueChunk() {
            return valueChunk;
        }
    }
}
