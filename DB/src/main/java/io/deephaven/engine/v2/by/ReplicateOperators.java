/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by;

import io.deephaven.compilertools.ReplicateUtilities;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.sources.DateTimeArraySource;
import io.deephaven.engine.v2.sources.LongArraySource;
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
        charToAllButBooleanAndFloats("DB/src/main/java/io/deephaven/engine/v2/by/SumCharChunk.java");
        charToAllButBooleanAndFloats("DB/src/main/java/io/deephaven/engine/v2/by/CharChunkedSumOperator.java");
        charToAllButBooleanAndFloats("DB/src/main/java/io/deephaven/engine/v2/by/CharChunkedAvgOperator.java");
        charToAllButBooleanAndFloats("DB/src/main/java/io/deephaven/engine/v2/by/CharChunkedVarOperator.java");
        floatToAllFloatingPoints("DB/src/main/java/io/deephaven/engine/v2/by/SumFloatChunk.java");
        floatToAllFloatingPoints("DB/src/main/java/io/deephaven/engine/v2/by/FloatChunkedSumOperator.java");
        floatToAllFloatingPoints("DB/src/main/java/io/deephaven/engine/v2/by/FloatChunkedAvgOperator.java");
        floatToAllFloatingPoints("DB/src/main/java/io/deephaven/engine/v2/by/FloatChunkedReAvgOperator.java");
        floatToAllFloatingPoints("DB/src/main/java/io/deephaven/engine/v2/by/FloatChunkedVarOperator.java");
        charToAllButBoolean("DB/src/main/java/io/deephaven/engine/v2/by/CharChunkedAddOnlyMinMaxOperator.java");
        charToAllButBoolean("DB/src/main/java/io/deephaven/engine/v2/utils/cast/CharToDoubleCast.java");
        replicateObjectAddOnlyMinMax();
        fixupLongAddOnlyMinMax();
        charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/v2/by/CharAddOnlySortedFirstOrLastChunkedOperator.java");
        charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/v2/by/CharStreamSortedFirstOrLastChunkedOperator.java");
        replicateObjectAddOnlyAndStreamSortedFirstLast();
    }

    private static void replicateObjectAddOnlyMinMax() throws IOException {
        final String objectAddOnlyMinMax = charToObject(
                "DB/src/main/java/io/deephaven/engine/v2/by/CharChunkedAddOnlyMinMaxOperator.java");
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
                new File("DB/src/main/java/io/deephaven/engine/v2/by/LongChunkedAddOnlyMinMaxOperator.java");
        List<String> lines = ReplicateUtilities
                .fixupChunkAttributes(FileUtils.readLines(longAddOnlyMinMaxFile, Charset.defaultCharset()));
        lines = ReplicateUtilities.globalReplacements(lines, "LongArraySource", "AbstractLongArraySource");
        lines = ReplicateUtilities.replaceRegion(lines, "extra constructor params",
                Collections.singletonList("            Class<?> type,"));
        lines = ReplicateUtilities.replaceRegion(lines, "resultColumn initialization", Collections.singletonList(
                "        resultColumn = type == DBDateTime.class ? new DateTimeArraySource() : new LongArraySource();"));
        lines = ReplicateUtilities.addImport(lines, DBDateTime.class, DateTimeArraySource.class, LongArraySource.class);
        FileUtils.writeLines(longAddOnlyMinMaxFile, lines);
    }

    private static void replicateObjectAddOnlyAndStreamSortedFirstLast() throws IOException {
        for (final String charClassJavaPath : new String[] {
                "DB/src/main/java/io/deephaven/engine/v2/by/CharAddOnlySortedFirstOrLastChunkedOperator.java",
                "DB/src/main/java/io/deephaven/engine/v2/by/CharStreamSortedFirstOrLastChunkedOperator.java"}) {
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
