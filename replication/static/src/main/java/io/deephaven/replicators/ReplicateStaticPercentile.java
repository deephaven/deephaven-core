//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.floatToAllFloatingPoints;
import static io.deephaven.replication.ReplicatePrimitiveCode.longToInt;
import static io.deephaven.replication.ReplicationUtils.addImport;
import static io.deephaven.replication.ReplicationUtils.removeDuplicateImports;
import static io.deephaven.replication.ReplicationUtils.replaceRegion;

/**
 * Replicates CharStaticPercentileOperator to the other primitive types. Int, long, float and double results may average
 * the two middle values, which their StaticPercentileAverage helpers (replicated from the long and float helpers)
 * compute; float and double groups that contain NaN produce NaN.
 */
public class ReplicateStaticPercentile {
    private static final String TASK = "replicateStaticPercentile";
    private static final String DIRECTORY =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/by/staticpercentile/";

    public static void main(String[] args) throws IOException {
        for (final String path : charToAllButBoolean(TASK, DIRECTORY + "CharStaticPercentileOperator.java")) {
            if (path.endsWith("IntStaticPercentileOperator.java")) {
                fixupIntegral(path, "Int", "IntegerArraySource");
            } else if (path.endsWith("LongStaticPercentileOperator.java")) {
                fixupIntegral(path, "Long", "LongArraySource");
            } else if (path.endsWith("FloatStaticPercentileOperator.java")) {
                fixupFloatingPoint(path, "Float");
            } else if (path.endsWith("DoubleStaticPercentileOperator.java")) {
                fixupFloatingPoint(path, "Double");
            }
        }
        longToInt(TASK, DIRECTORY + "LongStaticPercentileAverage.java");
        floatToAllFloatingPoints(TASK, DIRECTORY + "FloatStaticPercentileAverage.java");
    }

    /**
     * Int and long results that average the two middle values are doubles, held in a separate result column.
     *
     * @param path the replicated operator
     * @param type the capitalized name of the value type, Int or Long
     * @param resultSource the array source class for results that are not averaged
     */
    private static void fixupIntegral(final String path, final String type, final String resultSource)
            throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = replaceRegion(lines, "averageFields", List.of(
                "    private final boolean averageEvenlyDivided;",
                "    private final DoubleArraySource averagedResult;"));
        lines = replaceRegion(lines, "resultConstructor", List.of(
                "        this.averageEvenlyDivided = averageEvenlyDivided;",
                "        result = averageEvenlyDivided ? null : new " + resultSource + "();",
                "        averagedResult = averageEvenlyDivided ? new DoubleArraySource() : null;"));
        lines = replaceRegion(lines, "resultColumn", List.of(
                "        return averageEvenlyDivided ? averagedResult : result;"));
        lines = replaceRegion(lines, "averagedResult", averagedResult("averagedResult", "NULL_DOUBLE", type));
        lines = addImport(lines,
                "import io.deephaven.engine.table.impl.sources.DoubleArraySource;",
                "import static io.deephaven.util.QueryConstants.NULL_DOUBLE;");
        FileUtils.writeLines(file, removeDuplicateImports(lines));
    }

    /**
     * Float and double results have the value type whether or not they are averaged, so one result column serves both.
     * A group that receives a NaN produces NaN, so its values are released and it accumulates nothing more.
     *
     * @param path the replicated operator
     * @param type the capitalized name of the value type, Float or Double
     */
    private static void fixupFloatingPoint(final String path, final String type) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        final String primitive = type.toLowerCase();
        final String nullValue = "NULL_" + type.toUpperCase();
        lines = replaceRegion(lines, "averageFields", List.of(
                "    private final boolean averageEvenlyDivided;"));
        lines = replaceRegion(lines, "resultConstructor", List.of(
                "        this.averageEvenlyDivided = averageEvenlyDivided;",
                "        result = new " + type + "ArraySource();"));
        lines = replaceRegion(lines, "averagedResult", averagedResult("result", nullValue, type));
        lines = replaceRegion(lines, "nanFields", List.of(
                "    /**",
                "     * Stands in for the array of a destination that has received a NaN value; its result is NaN, and it",
                "     * accumulates nothing more.",
                "     */",
                "    private static final " + primitive + "[] NAN_RECEIVED = new " + primitive + "[0];"));
        lines = replaceRegion(lines, "skipDestination", List.of(
                "        if (array == NAN_RECEIVED) {",
                "            return;",
                "        }"));
        lines = replaceRegion(lines, "appendValue", List.of(
                "            if (" + type + ".isNaN(value)) {",
                "                arrays.getAndSetUnsafe(destination, NAN_RECEIVED);",
                "                return;",
                "            } else if (value != " + nullValue + ") {",
                "                array[size++] = value;",
                "            }"));
        lines = replaceRegion(lines, "nanResult", List.of(
                "        if (array == NAN_RECEIVED) {",
                "            result.set(destination, " + type + ".NaN);",
                "            return;",
                "        }"));
        FileUtils.writeLines(file, removeDuplicateImports(lines));
    }

    /**
     * @param column the result column that averaged results are written to
     * @param nullValue the null value of the result column
     * @param type the capitalized name of the value type, which names its {@code StaticPercentileAverage} helper
     */
    private static List<String> averagedResult(final String column, final String nullValue, final String type) {
        return List.of(
                "        if (averageEvenlyDivided) {",
                "            " + column + ".set(destination, size == 0 ? " + nullValue,
                "                    : " + type
                        + "StaticPercentileAverage.averagedPercentile(array, size, percentile));",
                "            return;",
                "        }");
    }
}
