/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.DateTimeArraySource;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.utils.cast.CharToDoubleCast;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ReplicateOperators {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(SumCharChunk.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(CharChunkedSumOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(CharChunkedAvgOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(CharChunkedVarOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(SumFloatChunk.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatChunkedSumOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatChunkedAvgOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatChunkedReAvgOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatChunkedVarOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(CharChunkedAddOnlyMinMaxOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(CharToDoubleCast.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        replicateObjectAddOnlyMinMax();
        fixupLongAddOnlyMinMax();
        ReplicatePrimitiveCode.charToAllButBoolean(
            CharAddOnlySortedFirstOrLastChunkedOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(CharStreamSortedFirstOrLastChunkedOperator.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        replicateObjectAddOnlyAndStreamSortedFirstLast();
    }

    private static void replicateObjectAddOnlyMinMax() throws IOException {
        final String objectAddOnlyMinMax = ReplicatePrimitiveCode
            .charToObject(CharChunkedAddOnlyMinMaxOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File objectAddOnlyMinMaxFile = new File(objectAddOnlyMinMax);
        List<String> lines = ReplicateUtilities.fixupChunkAttributes(
            FileUtils.readLines(objectAddOnlyMinMaxFile, Charset.defaultCharset()));
        lines = ReplicateUtilities.globalReplacements(lines, "QueryConstants.NULL_OBJECT", "null",
            "getObject", "get");
        lines = ReplicateUtilities.removeImport(lines, QueryConstants.class);
        lines = ReplicateUtilities.replaceRegion(lines, "extra constructor params",
            Collections.singletonList("            Class<?> type,"));
        lines = ReplicateUtilities.replaceRegion(lines, "resultColumn initialization",
            Collections.singletonList("        resultColumn = new ObjectArraySource<>(type);"));
        FileUtils.writeLines(objectAddOnlyMinMaxFile, lines);
    }

    private static void fixupLongAddOnlyMinMax() throws IOException {
        final String longBasePath = ReplicatePrimitiveCode.basePathForClass(
            LongChunkedAddOnlyMinMaxOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File longAddOnlyMinMaxFile = new File(longBasePath,
            LongChunkedAddOnlyMinMaxOperator.class.getSimpleName() + ".java");
        List<String> lines = ReplicateUtilities.fixupChunkAttributes(
            FileUtils.readLines(longAddOnlyMinMaxFile, Charset.defaultCharset()));
        lines = ReplicateUtilities.globalReplacements(lines, "LongArraySource",
            "AbstractLongArraySource");
        lines = ReplicateUtilities.replaceRegion(lines, "extra constructor params",
            Collections.singletonList("            Class<?> type,"));
        lines = ReplicateUtilities.replaceRegion(lines, "resultColumn initialization",
            Collections.singletonList(
                "        resultColumn = type == DBDateTime.class ? new DateTimeArraySource() : new LongArraySource();"));
        lines = ReplicateUtilities.addImport(lines, DBDateTime.class, DateTimeArraySource.class,
            LongArraySource.class);
        FileUtils.writeLines(longAddOnlyMinMaxFile, lines);
    }

    private static void replicateObjectAddOnlyAndStreamSortedFirstLast() throws IOException {
        for (final Class charClass : Arrays.asList(
            CharAddOnlySortedFirstOrLastChunkedOperator.class,
            CharStreamSortedFirstOrLastChunkedOperator.class)) {
            final String objectClassName =
                ReplicatePrimitiveCode.charToObject(charClass, ReplicatePrimitiveCode.MAIN_SRC);
            final File objectClassFile = new File(objectClassName);
            List<String> lines = ReplicateUtilities.fixupChunkAttributes(
                FileUtils.readLines(objectClassFile, Charset.defaultCharset()));
            lines = ReplicateUtilities.replaceRegion(lines, "sortColumnValues initialization",
                Collections.singletonList(
                    "        sortColumnValues = new ObjectArraySource<>(Object.class);"));
            FileUtils.writeLines(objectClassFile, lines);
        }
    }
}
