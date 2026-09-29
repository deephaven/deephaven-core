//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateParquetPushdownHandlers {
    private static final String TASK = "replicateParquetPushdownHandlers";

    private static final String PUSHDOWN_HANDLER_PATH =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/location/";
    private static final String CHAR_PUSHDOWN_HANDLER = PUSHDOWN_HANDLER_PATH + "CharPushdownHandler.java";
    private static final String FLOAT_PUSHDOWN_HANDLER = PUSHDOWN_HANDLER_PATH + "FloatPushdownHandler.java";

    /** The region {@link #dropCharOnlyNullLowerBound} strips, marked in {@code CharPushdownHandler}. */
    private static final String NULL_LOWER_BOUND_REGION = "null-lower-bound";

    /** Left in the generated file in that region's place. Names no type, so one text serves every replica. */
    private static final List<String> NULL_LOWER_BOUND_NOTE = List.of(
            "        // A null lower bound needs no reading of its own: the sentinel is MIN_VALUE, the domain's bottom.");

    /** Explains, in the generated file, why the long replica alone widens these. */
    private static final String INSTANT_NOTE = "    // Package-accessible, unlike the other replicas: Instant bounds"
            + " are epoch nanoseconds, so InstantPushdownHandler\n"
            + "    // reuses this arithmetic rather than duplicating it. See ReplicateParquetPushdownHandlers.\n";

    public static void main(String[] args) throws IOException {
        // char -> byte, short, int, long
        final List<String> integralHandlers = charToIntegers(TASK, CHAR_PUSHDOWN_HANDLER);
        for (final String integralHandler : integralHandlers) {
            dropCharOnlyNullLowerBound(integralHandler);
        }
        widenHelpersUsedByInstant(integralHandlers.stream()
                .filter(path -> path.endsWith("LongPushdownHandler.java"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("charToIntegers generated no long handler")));

        // float -> double
        floatToAllFloatingPoints(TASK, FLOAT_PUSHDOWN_HANDLER);
    }

    /**
     * Strips the char-only reading of a null lower bound from a generated file.
     * <p>
     * {@code CharPushdownHandler} has to read such a bound specially: {@code NULL_CHAR} is {@code Character.MAX_VALUE},
     * the top of the raw value domain, so handing it to the interval arithmetic as written would describe an empty
     * range. Every replica's sentinel is {@code MIN_VALUE}, already the bottom of its domain, where the ordinary range
     * test says exactly the right thing -- {@code X >= null} admits every value up to the upper bound and
     * {@code X > null} the same less the sentinel. So the branch is removed here rather than left in the replicas to be
     * skipped at runtime, and a note put in its place.
     */
    private static void dropCharOnlyNullLowerBound(final String handlerPath) throws IOException {
        final File handler = new File(handlerPath);
        final List<String> lines = FileUtils.readLines(handler, Charset.defaultCharset());
        if (lines.stream().noneMatch(line -> line.contains("region " + NULL_LOWER_BOUND_REGION))) {
            throw new IllegalStateException("No `" + NULL_LOWER_BOUND_REGION + "` region in " + handlerPath
                    + "; CharPushdownHandler must still mark its char-only null lower bound branch with one.");
        }
        FileUtils.writeLines(handler,
                ReplicationUtils.replaceRegion(lines, NULL_LOWER_BOUND_REGION, NULL_LOWER_BOUND_NOTE));
    }

    /**
     * {@code CharPushdownHandler} keeps its comparison helpers private, so every replica does too -- except the long
     * one. {@code InstantPushdownHandler} holds its bounds as epoch nanoseconds and reuses the long handler's
     * arithmetic rather than duplicating it, so those three methods must be visible to the package there.
     * <p>
     * Widened here rather than in the template, so that char, byte, short and int stay private, and annotated in the
     * generated file so a reader who notices the difference is not left guessing.
     */
    private static void widenHelpersUsedByInstant(final String longHandlerPath) throws IOException {
        final File longHandler = new File(longHandlerPath);
        final List<String> lines = ReplicationUtils.globalReplacements(
                FileUtils.readLines(longHandler, Charset.defaultCharset()),
                "    private static boolean maybeOverlapsRangeImpl\\(",
                INSTANT_NOTE + "    static boolean maybeOverlapsRangeImpl(",
                "    private static boolean maybeMatches\\(",
                INSTANT_NOTE + "    static boolean maybeMatches(",
                "    private static boolean maybeMatchesInverse\\(",
                INSTANT_NOTE + "    static boolean maybeMatchesInverse(");
        FileUtils.writeLines(longHandler, lines);
    }
}
