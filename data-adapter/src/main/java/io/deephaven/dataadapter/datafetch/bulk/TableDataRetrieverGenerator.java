//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.util.codegen.CodeGenerator;

/**
 * Generates a class that efficiently retrieves data from column sources.
 */
class TableDataRetrieverGenerator {

    private static final String COMPILED_CLASS_NAME = "TableDataArrayRetriever";
    private final String[] colTypeNames;
    private final boolean[] colIsPrimitive;

    public TableDataRetrieverGenerator(ColumnSource<?>[] colSources) {
        // Create an array of the columns' type names. Sanity check the columns as well.
        colTypeNames = new String[colSources.length];
        colIsPrimitive = new boolean[colSources.length];

        for (int colIdx = 0; colIdx < colSources.length; colIdx++) {
            try {
                ColumnSource<?> cs = colSources[colIdx];

                final Class<?> colType = cs.getType();
                Assert.eqFalse(colType.isAnonymousClass(), "colType.isAnonymousClass()");
                Assert.eqFalse(colType.isLocalClass(), "colType.isLocalClass()");
                String colTypeName = colType.getCanonicalName();
                Assert.neqNull(colTypeName, "colTypeName");

                colTypeNames[colIdx] = colTypeName;
                colIsPrimitive[colIdx] = colType.isPrimitive();
            } catch (AssertionFailure ex) {
                throw new IllegalArgumentException("Column " + colIdx + " is invalid", ex);
            }
        }
    }

    public Class<?> generate() {
        final String classBody = generateClassBody().build();
        final String desc = "TableDataArrayRetriever[" + String.join(", ", colTypeNames);
        return compile(desc, classBody);
    }

    private Class<?> compile(final String desc, final String classBody) {
        try (final QueryPerformanceNugget nugget = QueryPerformanceRecorder.getInstance()
                .getNugget(TableDataRetrieverGenerator.class.getName() + "Compile: " + desc)) {
            // Compilation needs to take place with elevated privileges, but the created object should not have them.
            return ExecutionContext.getContext().getQueryCompiler().compile(
                    QueryCompilerRequest.builder()
                            .className(COMPILED_CLASS_NAME)
                            .classBody(classBody)
                            .packageNameRoot(QueryCompilerImpl.DYNAMIC_CLASS_PREFIX)
                            .description(desc)
                            .build());
        }
    }

    private CodeGenerator generateClassBody() {
        final CodeGenerator g = CodeGenerator.create(
                getImportStatements(), "",
                "import java.lang.*;", "",
                "public class $CLASSNAME$ extends [[ABSTRACT_MULTIROW_RECORD_ADAPTER_CANONICAL]]", CodeGenerator.block(
                        generateConstructor(), "",
                        generateCreateDataArrays(), "",
                        generatePopulateArrsForRowSequence(), ""));

        g.replace("ABSTRACT_MULTIROW_RECORD_ADAPTER_CANONICAL",
                AbstractGeneratedTableDataArrayRetriever.class.getCanonicalName());

        return g.freeze();
    }

    private static String getImportStatements() {
        final Class<?>[] importClasses = {
                io.deephaven.base.verify.Assert.class,
                io.deephaven.dataadapter.ChunkToArrayUtil.class,
                io.deephaven.dataadapter.ContextHolder.class,
                io.deephaven.engine.table.ColumnSource.class,
                io.deephaven.engine.rowset.RowSequence.class,
        };
        final StringBuilder sb = new StringBuilder();
        for (Class<?> clazz : importClasses) {
            sb.append("import ").append(clazz.getCanonicalName()).append(";\n");
        }
        return sb.toString();
    }

    private CodeGenerator generateConstructor() {
        final CodeGenerator g = CodeGenerator.create(
                "public $CLASSNAME$(final ColumnSource<?>[] colSources)", CodeGenerator.block(
                        "super(colSources);",
                        CodeGenerator.repeated("validateColumnArg",
                                "if ( ![[TYPE]].class.isAssignableFrom(colSources[[[IDX]]].getType()) )",
                                CodeGenerator.block(
                                        "throw new IllegalArgumentException(\"Column [[IDX]]: Expected type [[TYPE]], instead found type \" +",
                                        "colSources[[[IDX]]].getType().getCanonicalName());"))));

        for (int colIdx = 0; colIdx < colTypeNames.length; colIdx++) {
            final String typeName = colTypeNames[colIdx];
            final CodeGenerator validateColumnArg = g.instantiateNewRepeated("validateColumnArg");
            validateColumnArg.replace("IDX", String.valueOf(colIdx));
            validateColumnArg.replace("TYPE", typeName);
            validateColumnArg.freeze();
        }

        return g.freeze();
    }

    private CodeGenerator generateCreateDataArrays() {
        final CodeGenerator g = CodeGenerator.create(
                "@Override",
                "public final Object[] createDataArrays(final int len)", CodeGenerator.block(
                        "final int nCols = columnSources.length;",
                        "final Object[] dataArrs = new Object[nCols];",
                        "",
                        CodeGenerator.repeated("arrInitializer", "dataArrs[[[IDX]]] = new [[TYPE]][len];"),
                        "",
                        "return dataArrs;"));

        for (int colIdx = 0; colIdx < colTypeNames.length; colIdx++) {
            final String typeName = colTypeNames[colIdx];
            final CodeGenerator arrInitializer = g.instantiateNewRepeated("arrInitializer");
            arrInitializer.replace("IDX", String.valueOf(colIdx));
            arrInitializer.replace("TYPE", typeName);
            arrInitializer.freeze();
        }

        return g.freeze();
    }

    private CodeGenerator generatePopulateArrsForRowSequence() {
        CodeGenerator g = CodeGenerator.create(
                "@SuppressWarnings({\"unchecked\", \"rawtypes\"})",
                "@Override",
                "protected final void populateArrsForRowSequence(",
                "boolean usePrev,",
                "Object[] dataArrs,",
                "int arrIdx,",
                "ContextHolder contextHolder,",
                "RowSequence rowSequence,",
                "int rowSequenceSize)", CodeGenerator.block(
                        "Assert.eq(dataArrs.length, \"dataArrs.length\", columnSources.length, \"columnSources.length\");",
                        CodeGenerator.repeated("dataArrayPopulator",
                                "ChunkToArrayUtil.[[METHOD_NAME]](",
                                "columnSources[[[IDX]]],",
                                "rowSequence,",
                                "rowSequenceSize,",
                                "contextHolder.getGetContext([[IDX]]),",
                                "([[TYPE]][]) dataArrs[[[IDX]]],",
                                "arrIdx,",
                                "usePrev",
                                ");")));

        for (int colIdx = 0; colIdx < colTypeNames.length; colIdx++) {
            final String typeName = colTypeNames[colIdx];
            final CodeGenerator arrPopulator = g.instantiateNewRepeated("dataArrayPopulator");
            arrPopulator.replace("IDX", String.valueOf(colIdx));
            arrPopulator.replace("TYPE", typeName);
            arrPopulator.replace("METHOD_NAME",
                    colIsPrimitive[colIdx] ? "populateArrFromChunk" : "populateObjArrFromChunk");
            arrPopulator.freeze();
        }

        return g.freeze();
    }

}
