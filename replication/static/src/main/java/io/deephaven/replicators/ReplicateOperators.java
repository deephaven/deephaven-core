//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import io.deephaven.util.QueryConstants;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateOperators {
    private static final String TASK = "replicateOperators";

    public static void main(String[] args) throws IOException {
        charToAllButBooleanAndFloats(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/SumCharChunk.java");
        charToAllButBooleanAndFloats(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedSumOperator.java");
        charToAllButBooleanAndFloats(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedAvgOperator.java");
        charToAllButBooleanAndFloats(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedVarOperator.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/SumFloatChunk.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/FloatChunkedSumOperator.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/FloatChunkedAvgOperator.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/FloatChunkedReAvgOperator.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/FloatChunkedVarOperator.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedAddOnlyMinMaxOperator.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/cast/CharToDoubleCast.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/cast/CharToBigDecimalCast.java");
        replicateObjectAddOnlyMinMax();
        fixupLongAddOnlyMinMax();
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharAddOnlySortedFirstOrLastChunkedOperator.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharBlinkSortedFirstOrLastChunkedOperator.java");
        replicateObjectAddOnlyAndBlinkSortedFirstLast();
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/alternatingcolumnsource/CharAlternatingColumnSourceUnorderedMergeKernel.java");
        replicateObjectUnorderedMergeKernel();
    }

    private static void replicateObjectAddOnlyMinMax() throws IOException {
        final String objectAddOnlyMinMax = charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedAddOnlyMinMaxOperator.java");
        final File objectAddOnlyMinMaxFile = new File(objectAddOnlyMinMax);
        List<String> lines = ReplicationUtils
                .fixupChunkAttributes(FileUtils.readLines(objectAddOnlyMinMaxFile, Charset.defaultCharset()));
        lines = ReplicationUtils.globalReplacements(lines, "QueryConstants.NULL_OBJECT", "null", "getObject", "get");
        lines = ReplicationUtils.removeImport(lines, QueryConstants.class);
        lines = ReplicationUtils.replaceRegion(lines, "extra constructor params",
                Collections.singletonList("            Class<?> type,"));
        lines = ReplicationUtils.replaceRegion(lines, "resultColumn initialization",
                Collections.singletonList("        resultColumn = new ObjectArraySource<>(type);"));
        FileUtils.writeLines(objectAddOnlyMinMaxFile, lines);
    }

    private static final String resultInitReplacementForLong = "" +
            "        if (type == Instant.class) {\n" +
            "            actualResult = new InstantArraySource();\n" +
            "            resultColumn = ((NanosBasedTimeArraySource<?>)actualResult).toEpochNano();\n" +
            "        } else {\n" +
            "            actualResult = resultColumn = new LongArraySource();\n" +
            "        }";

    private static void fixupLongAddOnlyMinMax() throws IOException {
        final File longAddOnlyMinMaxFile =
                new File(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/LongChunkedAddOnlyMinMaxOperator.java");
        List<String> lines = ReplicationUtils
                .fixupChunkAttributes(FileUtils.readLines(longAddOnlyMinMaxFile, Charset.defaultCharset()));
        lines = ReplicationUtils.replaceRegion(lines, "actualResult", Collections.singletonList(
                "    private final ColumnSource<?> actualResult;"));
        lines = ReplicationUtils.replaceRegion(lines, "extra constructor params",
                Collections.singletonList("            Class<?> type,"));
        lines = ReplicationUtils.addImport(lines,
                "import java.time.Instant;",
                "import io.deephaven.engine.table.impl.sources.InstantArraySource;",
                "import io.deephaven.engine.table.impl.sources.NanosBasedTimeArraySource;");
        lines = ReplicationUtils.replaceRegion(lines, "resultColumn initialization",
                Collections.singletonList(resultInitReplacementForLong));
        lines = ReplicationUtils.replaceRegion(lines, "getResultColumns", Collections.singletonList(
                "        return Collections.<String, ColumnSource<?>>singletonMap(name, actualResult);"));
        FileUtils.writeLines(longAddOnlyMinMaxFile, lines);
    }

    private static void replicateObjectAddOnlyAndBlinkSortedFirstLast() throws IOException {
        for (final String charClassJavaPath : new String[] {
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharAddOnlySortedFirstOrLastChunkedOperator.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharBlinkSortedFirstOrLastChunkedOperator.java"}) {
            final String objectClassName =
                    charToObject(TASK, charClassJavaPath);
            final File objectClassFile = new File(objectClassName);
            List<String> lines = ReplicationUtils
                    .fixupChunkAttributes(FileUtils.readLines(objectClassFile, Charset.defaultCharset()));
            lines = ReplicationUtils.replaceRegion(lines, "sortColumnValues initialization",
                    Collections.singletonList("        sortColumnValues = new ObjectArraySource<>(Object.class);"));
            FileUtils.writeLines(objectClassFile, lines);
        }
    }

    private static void replicateObjectUnorderedMergeKernel() throws IOException {
        final String objectUnorderedMerge = charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/alternatingcolumnsource/CharAlternatingColumnSourceUnorderedMergeKernel.java");
        final File objectUnorderedMergeFile = new File(objectUnorderedMerge);
        List<String> lines = ReplicationUtils
                .fixupChunkAttributes(FileUtils.readLines(objectUnorderedMergeFile, Charset.defaultCharset()));
        FileUtils.writeLines(objectUnorderedMergeFile, lines);
    }
}
