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
import java.util.Collections;
import java.util.List;

public class ReplicateOperators {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(SumCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(CharChunkedSumOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(CharChunkedAvgOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(CharChunkedVarOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(SumFloatChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatChunkedSumOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatChunkedAvgOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatChunkedReAvgOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatChunkedVarOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(CharChunkedAppendOnlyMinMaxOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(CharToDoubleCast.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateObjectAppendMinMax();
        fixupLongAppendMinMax();
    }

    private static void replicateObjectAppendMinMax() throws IOException {
        final String objectAppendOnlyMinMax = ReplicatePrimitiveCode.charToObject(CharChunkedAppendOnlyMinMaxOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File objectAppendMinMaxFile = new File(objectAppendOnlyMinMax);
        List<String> lines = ReplicateUtilities.fixupChunkAttributes(FileUtils.readLines(objectAppendMinMaxFile, Charset.defaultCharset()));
        lines = ReplicateUtilities.globalReplacements(lines, "QueryConstants.NULL_OBJECT", "null", "getObject", "get");
        lines = ReplicateUtilities.removeImport(lines, QueryConstants.class);
        lines = ReplicateUtilities.replaceRegion(lines, "extra constructor params", Collections.singletonList("            Class<?> type,"));
        lines = ReplicateUtilities.replaceRegion(lines, "resultColumn initialization", Collections.singletonList("        resultColumn = new ObjectArraySource<>(type);"));
        FileUtils.writeLines(objectAppendMinMaxFile, lines);
    }

    private static void fixupLongAppendMinMax() throws IOException {
        final String longBasePath = ReplicatePrimitiveCode.basePathForClass(LongChunkedAppendOnlyMinMaxOperator.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File longAppendMinMaxFile = new File(longBasePath, LongChunkedAppendOnlyMinMaxOperator.class.getSimpleName() + ".java");
        List<String> lines = ReplicateUtilities.fixupChunkAttributes(FileUtils.readLines(longAppendMinMaxFile, Charset.defaultCharset()));
        lines = ReplicateUtilities.globalReplacements(lines, "LongArraySource", "AbstractLongArraySource");
        lines = ReplicateUtilities.replaceRegion(lines, "extra constructor params", Collections.singletonList("            Class<?> type,"));
        lines = ReplicateUtilities.replaceRegion(lines, "resultColumn initialization", Collections.singletonList("        resultColumn = type == DBDateTime.class ? new DateTimeArraySource() : new LongArraySource();"));
        lines = ReplicateUtilities.addImport(lines, DBDateTime.class, DateTimeArraySource.class, LongArraySource.class);
        FileUtils.writeLines(longAppendMinMaxFile, lines);
    }
}
