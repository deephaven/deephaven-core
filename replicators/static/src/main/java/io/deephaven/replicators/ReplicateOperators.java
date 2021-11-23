/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import io.deephaven.compilertools.ReplicateUtilities;
import io.deephaven.engine.time.DateTime;
import io.deephaven.engine.table.impl.sources.DateTimeArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.util.QueryConstants;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.*;

public class ReplicateOperators {
    public static void main(String[] args) throws IOException {
        charToAllButBooleanAndFloats("engine/table/src/main/java/io/deephaven/engine/table/impl/by/SumCharChunk.java");
        charToAllButBooleanAndFloats("engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedSumOperator.java");
        charToAllButBooleanAndFloats("engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedAvgOperator.java");
        charToAllButBooleanAndFloats("engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedVarOperator.java");
        floatToAllFloatingPoints("engine/table/src/main/java/io/deephaven/engine/table/impl/by/SumFloatChunk.java");
        floatToAllFloatingPoints("engine/table/src/main/java/io/deephaven/engine/table/impl/by/FloatChunkedSumOperator.java");
        floatToAllFloatingPoints("engine/table/src/main/java/io/deephaven/engine/table/impl/by/FloatChunkedAvgOperator.java");
        floatToAllFloatingPoints("engine/table/src/main/java/io/deephaven/engine/table/impl/by/FloatChunkedReAvgOperator.java");
        floatToAllFloatingPoints("engine/table/src/main/java/io/deephaven/engine/table/impl/by/FloatChunkedVarOperator.java");
        charToAllButBoolean("engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedAddOnlyMinMaxOperator.java");
        charToAllButBoolean("engine/table/src/main/java/io/deephaven/engine/table/impl/utils/cast/CharToDoubleCast.java");
        replicateObjectAddOnlyMinMax();
        fixupLongAddOnlyMinMax();
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharAddOnlySortedFirstOrLastChunkedOperator.java");
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharStreamSortedFirstOrLastChunkedOperator.java");
        replicateObjectAddOnlyAndStreamSortedFirstLast();
    }

    private static void replicateObjectAddOnlyMinMax() throws IOException {
        final String objectAddOnlyMinMax = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharChunkedAddOnlyMinMaxOperator.java");
        final File objectAddOnlyMinMaxFile = new File(objectAddOnlyMinMax);
        List<String> lines = ReplicateUtilities
                .fixupChunkAttributes(FileUtils.readLines(objectAddOnlyMinMaxFile, Charset.defaultCharset()));
        lines = ReplicateUtilities.globalReplacements(lines, "QueryConstants.NULL_OBJECT", "null", "getObject", "get");
        lines = ReplicateUtilities.removeImport(lines, QueryConstants.class);
        lines = ReplicateUtilities.replaceRegion(lines, "extra constructor params",
                Collections.singletonList("            Class<?> type,"));
        lines = ReplicateUtilities.replaceRegion(lines, "resultColumn initialization",
                Collections.singletonList("        resultColumn = new ObjectArraySource<>(type);"));
        FileUtils.writeLines(objectAddOnlyMinMaxFile, lines);
    }

    private static void fixupLongAddOnlyMinMax() throws IOException {
        final File longAddOnlyMinMaxFile =
                new File("engine/table/src/main/java/io/deephaven/engine/table/impl/by/LongChunkedAddOnlyMinMaxOperator.java");
        List<String> lines = ReplicateUtilities
                .fixupChunkAttributes(FileUtils.readLines(longAddOnlyMinMaxFile, Charset.defaultCharset()));
        lines = ReplicateUtilities.globalReplacements(lines, "LongArraySource", "AbstractLongArraySource");
        lines = ReplicateUtilities.replaceRegion(lines, "extra constructor params",
                Collections.singletonList("            Class<?> type,"));
        lines = ReplicateUtilities.replaceRegion(lines, "resultColumn initialization", Collections.singletonList(
                "        resultColumn = type == DateTime.class ? new DateTimeArraySource() : new LongArraySource();"));
        lines = ReplicateUtilities.addImport(lines, DateTime.class, DateTimeArraySource.class, LongArraySource.class);
        FileUtils.writeLines(longAddOnlyMinMaxFile, lines);
    }

    private static void replicateObjectAddOnlyAndStreamSortedFirstLast() throws IOException {
        for (final String charClassJavaPath : new String[] {
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharAddOnlySortedFirstOrLastChunkedOperator.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/CharStreamSortedFirstOrLastChunkedOperator.java"}) {
            final String objectClassName =
                    charToObject(charClassJavaPath);
            final File objectClassFile = new File(objectClassName);
            List<String> lines = ReplicateUtilities
                    .fixupChunkAttributes(FileUtils.readLines(objectClassFile, Charset.defaultCharset()));
            lines = ReplicateUtilities.replaceRegion(lines, "sortColumnValues initialization",
                    Collections.singletonList("        sortColumnValues = new ObjectArraySource<>(Object.class);"));
            FileUtils.writeLines(objectClassFile, lines);
        }
    }
}
