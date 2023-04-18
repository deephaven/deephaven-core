package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateUpdateBy {
    public static void main(String[] args) throws IOException {
        List<String> files = ReplicatePrimitiveCode.charToAll(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/fill/CharFillByOperator.java");
        for (final String f : files) {
            if (f.contains("Int")) {
                fixupInteger(f);
            }

            if (f.contains("Long")) {
                augmentLongWithReinterps(f);
            }

            if (f.contains("Boolean")) {
                fixupBoolean(f);
            }
        }

        String objectResult = ReplicatePrimitiveCode.charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/fill/CharFillByOperator.java");
        fixupStandardObject(objectResult, "ObjectFillByOperator", false,
                "super\\(fillPair, new String\\[\\] \\{ fillPair.rightColumn \\}, rowRedirection\\);",
                "super(fillPair, new String[] { fillPair.rightColumn }, rowRedirection, colType);",
                " BaseObjectUpdateByOperator", " BaseObjectUpdateByOperator<T>",
                "public ObjectChunk<Object,", "public ObjectChunk<T,");

        final String[] exemptions = new String[] {
                "long singletonGroup = QueryConstants.NULL_LONG",
                "long smallestModifiedKey",
                "LongChunk<OrderedRowKeys>",
                "LongChunk<\\? extends RowKeys>",
                "long groupPosition",
                "long bucketPosition",
                "long firstUnmodifiedKey",
                "long getFirstReprocessKey"
        };

        files = ReplicatePrimitiveCode.charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/internal/BaseCharUpdateByOperator.java",
                exemptions);
        for (final String f : files) {
            if (f.contains("Int")) {
                fixupInteger(f);
            }

            if (f.contains("Byte")) {
                fixupByteBase(f);
            }
        }
        objectResult = ReplicatePrimitiveCode.charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/internal/BaseCharUpdateByOperator.java");
        fixupStandardObject(objectResult, "BaseObjectUpdateByOperator", true,
                "this\\(pair, affectingColumns, rowRedirection, null, 0, 0, false\\);",
                "this(pair, affectingColumns, rowRedirection, null, 0, 0, false, colType);");

        replicateNumericOperator(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/sum/ShortCumSumOperator.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/sum/FloatCumSumOperator.java");

        files = ReplicatePrimitiveCode.shortToAllNumericals(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/minmax/ShortCumMinMaxOperator.java",
                null);
        for (final String f : files) {
            if (f.contains("Integer")) {
                fixupInteger(f);
            }

            if (f.contains("Byte")) {
                fixupByte(f);
            }

            if (f.contains("Long")) {
                augmentLongWithReinterps(f);
            }
        }

        replicateNumericOperator(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/prod/ShortCumProdOperator.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/prod/FloatCumProdOperator.java");

        files = ReplicatePrimitiveCode.charToAllButBooleanAndFloats(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/ema/CharEMAOperator.java");
        for (final String f : files) {
            if (f.contains("Integer")) {
                fixupInteger(f);
            }
            if (f.contains("Byte")) {
                fixupByte(f);
            }
        }
        ReplicatePrimitiveCode.floatToAllFloatingPoints(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/ema/FloatEMAOperator.java");

        files = ReplicatePrimitiveCode.charToAllButBooleanAndFloats(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/emsum/CharEMSOperator.java");
        for (final String f : files) {
            if (f.contains("Integer")) {
                fixupInteger(f);
            }
            if (f.contains("Byte")) {
                fixupByte(f);
            }
        }
        ReplicatePrimitiveCode.floatToAllFloatingPoints(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/emsum/FloatEMSOperator.java");

        replicateNumericOperator(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/rollingsum/ShortRollingSumOperator.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/rollingsum/FloatRollingSumOperator.java");

        files = ReplicatePrimitiveCode.charToIntegers(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/rollingavg/CharRollingAvgOperator.java",
                exemptions);
        for (final String f : files) {
            if (f.contains("Int")) {
                fixupInteger(f);
            }

            if (f.contains("Byte")) {
                fixupByte(f);
            }
        }
        ReplicatePrimitiveCode.floatToAllFloatingPoints(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/rollingavg/FloatRollingAvgOperator.java");

        files = ReplicatePrimitiveCode.charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/rollingminmax/CharRollingMinMaxOperator.java",
                exemptions);
        for (final String f : files) {
            if (f.contains("Int")) {
                fixupInteger(f);
            }
        }

        files = ReplicatePrimitiveCode.charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/rollingproduct/CharRollingProductOperator.java");
        for (final String f : files) {
            if (f.contains("Integer")) {
                fixupInteger(f);
            }
        }

        files = ReplicatePrimitiveCode.charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/delta/CharDeltaOperator.java",
                exemptions);
        for (final String f : files) {
            if (f.contains("Int")) {
                fixupInteger(f);
            }
        }

        files = ReplicatePrimitiveCode.charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/updateby/rollingcount/CharRollingCountOperator.java");
        for (final String f : files) {
            if (f.contains("Integer")) {
                fixupInteger(f);
            }

            if (f.contains("Byte")) {
                fixupByte(f);
            }
        }
    }

    private static void replicateNumericOperator(@NotNull final String shortClass, @NotNull final String floatClass)
            throws IOException {
        for (final String f : ReplicatePrimitiveCode.shortToAllIntegralTypes(shortClass)) {
            if (f.contains("Integer")) {
                fixupInteger(f);
            }

            if (f.contains("Byte")) {
                fixupByte(f);
            }

            if (f.contains("Long") && f.contains("MinMax")) {
                augmentLongWithReinterps(f);
            }
        }

        ReplicatePrimitiveCode.floatToAllFloatingPoints(floatClass);
    }

    private static void fixupByteBase(String byteResult) throws IOException {
        final File objectFile = new File(byteResult);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        lines = addImport(lines, "import io.deephaven.util.QueryConstants;",
                "import io.deephaven.engine.table.impl.sources.ByteArraySource;",
                "import io.deephaven.engine.table.impl.sources.ByteSparseArraySource;",
                "import io.deephaven.engine.table.WritableColumnSource;");

        lines = replaceRegion(lines, "extra-fields", Collections.singletonList("    final byte nullValue;"));
        lines = replaceRegion(lines, "constructor",
                Collections.singletonList("        this.nullValue = getNullValue();"));
        lines = globalReplacements(lines,
                "QueryConstants.NULL_BYTE", "nullValue",
                "protected ByteArraySource bucketLastVal;", "protected WritableColumnSource<Byte> bucketLastVal;");

        lines = replaceRegion(lines, "Shifts",
                Collections.singletonList(
                        "    @Override\n" +
                                "    public void applyOutputShift(@NotNull final RowSet subIndexToShift, final long delta) {\n"
                                +
                                "        if (outputSource instanceof BooleanSparseArraySource.ReinterpretedAsByte) {\n"
                                +
                                "            ((BooleanSparseArraySource.ReinterpretedAsByte)outputSource).shift(subIndexToShift, delta);\n"
                                +
                                "        } else {\n" +
                                "            ((ByteSparseArraySource)outputSource).shift(subIndexToShift, delta);\n" +
                                "        }\n" +
                                "    }"));

        lines = replaceRegion(lines, "extra-methods",
                Collections.singletonList(
                        "    protected byte getNullValue() {\n" +
                                "        return QueryConstants.NULL_BYTE;\n" +
                                "    }\n" +
                                "\n" +
                                "    // region extra-methods\n" +
                                "    protected WritableColumnSource<Byte> makeSparseSource() {\n" +
                                "        return new ByteSparseArraySource();\n" +
                                "    }\n" +
                                "\n" +
                                "    protected WritableColumnSource<Byte> makeDenseSource() {\n" +
                                "        return new ByteArraySource();\n" +
                                "    }"));
        lines = replaceRegion(lines, "create-dense", Collections.singletonList(
                "            this.maybeInnerSource = makeDenseSource();"));
        lines = replaceRegion(lines, "create-sparse", Collections.singletonList(
                "            this.outputSource = makeSparseSource();"));

        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupBoolean(String boolResult) throws IOException {
        final File objectFile = new File(boolResult);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;",
                "import java.util.Map;",
                "import java.util.Collections;",
                "import io.deephaven.engine.table.impl.sources.BooleanArraySource;",
                "import io.deephaven.engine.table.impl.sources.BooleanSparseArraySource;",
                "import io.deephaven.engine.table.WritableColumnSource;");

        lines = globalReplacements(lines,
                "BaseBooleanUpdateByOperator", "BaseByteUpdateByOperator",
                "boolean singletonVal", "byte singletonVal",
                "QueryConstants", "BooleanUtils",
                "boolean curVal", "byte curVal",
                "boolean val", "byte val",
                "getBoolean", "getByte",
                "boolean previousVal", "byte previousVal",
                "boolean currentVal", "byte currentVal",
                "BooleanChunk", "ByteChunk",
                "NULL_BOOLEAN", "NULL_BOOLEAN_AS_BYTE");
        lines = globalReplacements(lines,
                "!BooleanPrimitives\\.isNull\\(currentVal\\)", "currentVal != NULL_BOOLEAN_AS_BYTE");
        lines = replaceRegion(lines, "extra-methods",
                Collections.singletonList(
                        "    @Override\n" +
                                "    protected byte getNullValue() {\n" +
                                "        return NULL_BOOLEAN_AS_BYTE;\n" +
                                "    }\n" +
                                "    @Override\n" +
                                "    protected WritableColumnSource<Byte> makeSparseSource() {\n" +
                                "        return (WritableColumnSource<Byte>) new BooleanSparseArraySource().reinterpret(byte.class);\n"
                                +
                                "    }\n" +
                                "\n" +
                                "    @Override\n" +
                                "    protected WritableColumnSource<Byte> makeDenseSource() {\n" +
                                "        return (WritableColumnSource<Byte>) new BooleanArraySource().reinterpret(byte.class);\n"
                                +
                                "    }\n" +
                                "\n" +
                                "    @NotNull\n" +
                                "    @Override\n" +
                                "    public Map<String, ColumnSource<?>> getOutputColumns() {\n" +
                                "        return Collections.singletonMap(pair.leftColumn, outputSource.reinterpret(Boolean.class));\n"
                                +
                                "    }"));
        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupByte(String byteResult) throws IOException {
        final File objectFile = new File(byteResult);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = replaceRegion(lines, "extra-fields", Collections.singletonList("    final byte nullValue;"));
        lines = replaceRegion(lines, "extra-constructor-args",
                Collections.singletonList("                               ,final byte nullValue"));
        lines = replaceRegion(lines, "constructor", Collections.singletonList("        this.nullValue = nullValue;"));
        lines = ReplicationUtils.globalReplacements(lines,
                "!= NULL_BYTE", "!= nullValue",
                "== NULL_BYTE", "== nullValue");
        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupInteger(String intResult) throws IOException {
        final File objectFile = new File(intResult);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines,
                "BaseIntegerUpdateByOperator", "BaseIntUpdateByOperator",
                "BaseWindowedIntegerUpdateByOperator", "BaseWindowedIntUpdateByOperator",
                "public class Integer", "public class Int",
                "public Integer", "public Int",
                "WritableIntegerChunk", "WritableIntChunk",
                "IntegerChunk", "IntChunk",
                "getInteger", "getInt",
                "IntegerRingBuffer", "IntRingBuffer",
                "SizedIntegerChunk", "SizedIntChunk");
        if (intResult.contains("Integer")) {
            FileUtils.writeLines(new File(intResult.replaceAll("Integer", "Int")), lines);
            FileUtils.deleteQuietly(objectFile);
        } else {
            FileUtils.writeLines(objectFile, lines);
        }
    }

    private static void fixupStandardObject(String objectResult, final String className,
            boolean augmentConstructorAndFields,
            String... extraReplacements) throws IOException {
        final File objectFile = new File(objectResult);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = fixupChunkAttributes(lines);
        lines = ReplicationUtils.addImport(lines, "import io.deephaven.engine.table.impl.util.ChunkUtils;");
        try {
            lines = removeImport(lines, "import static io.deephaven.util.QueryConstants.NULL_OBJECT;");
        } catch (Exception e) {
            // Hey' it's fiiiiine. Don't worrrryy about it!
        }
        lines = ReplicationUtils.globalReplacements(lines,
                "class " + className, "class " + className + "<T>",
                "WritableColumnSource<Object>", "WritableColumnSource<T>",
                " ObjectSparseArraySource ", " ObjectSparseArraySource<T> ",
                " ObjectArraySource ", " ObjectArraySource<T> ",
                "ObjectChunk<Object, Values>", "ObjectChunk<T, Values>",
                "SizedObjectChunk<Object, >", "SizedObjectChunk<>",
                "new ObjectArraySource\\(\\);", "new ObjectArraySource<>(colType);",
                "new ObjectSparseArraySource\\(\\);", "new ObjectSparseArraySource<>(colType);",
                "(?:QueryConstants\\.)?NULL_OBJECT", "null",
                "Object lastValidValue", "T lastValidValue",
                "Object val", "T val",
                "Object curVal", "T curVal",
                "Object previousVal", "T previousVal",
                "Object singletonVal", "T singletonVal",
                "getObject", "get",
                "getPrevObject", "getPrev");
        if (extraReplacements != null && extraReplacements.length > 0) {
            lines = globalReplacements(lines, extraReplacements);
        }
        lines = ReplicationUtils.replaceRegion(lines, "extra-constructor-args",
                Collections.singletonList("                                      , final Class<T> colType"));
        lines = ReplicationUtils.replaceRegion(lines, "clear-output",
                Collections.singletonList(
                        "    @Override\n" +
                                "    public void clearOutputRows(final RowSet toClear) {\n" +
                                "        // if we are redirected, clear the inner source\n" +
                                "        if (rowRedirection != null) {\n" +
                                "            ChunkUtils.fillWithNullValue(maybeInnerSource, toClear);\n" +
                                "        } else {\n" +
                                "            ChunkUtils.fillWithNullValue(outputSource, toClear);\n" +
                                "        }\n" +
                                "    }"));


        if (augmentConstructorAndFields) {
            lines = ReplicationUtils.replaceRegion(lines, "extra-fields",
                    Collections.singletonList("    private final Class<T> colType;"));
            lines = ReplicationUtils.replaceRegion(lines, "constructor",
                    Collections.singletonList("        this.colType = colType;"));
        }
        FileUtils.writeLines(objectFile, lines);
    }

    private static void augmentLongWithReinterps(final String longResult) throws IOException {
        final File objectFile = new File(longResult);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;",
                "import java.util.Map;",
                "import java.util.Collections;",
                "import io.deephaven.time.DateTime;",
                "import java.time.Instant;",
                "import io.deephaven.engine.table.impl.sources.ReinterpretUtils;");
        lines = replaceRegion(lines, "extra-fields",
                Collections.singletonList("    private final Class<?> type;"));
        lines = replaceRegion(lines, "extra-constructor-args",
                Collections.singletonList("                              ,@NotNull final Class<?> type"));
        lines = replaceRegion(lines, "constructor",
                Collections.singletonList("        this.type = type;"));
        lines = replaceRegion(lines, "extra-methods",
                Collections.singletonList(
                        "    @NotNull\n" +
                                "    @Override\n" +
                                "    public Map<String, ColumnSource<?>> getOutputColumns() {\n" +
                                "        final ColumnSource<?> actualOutput;\n" +
                                "        if(type == DateTime.class) {\n" +
                                "            actualOutput = ReinterpretUtils.longToDateTimeSource(outputSource);\n" +
                                "        } else {\n" +
                                "            actualOutput = outputSource;\n" +
                                "        }\n" +
                                "        return Collections.singletonMap(pair.leftColumn, actualOutput);\n" +
                                "    }"));
        FileUtils.writeLines(objectFile, lines);
    }
}
