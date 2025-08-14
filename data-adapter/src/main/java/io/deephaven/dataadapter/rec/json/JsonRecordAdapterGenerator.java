//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.dataadapter.datafetch.bulk.PartitionedTableDataArrayRetrieverImpl;
import io.deephaven.dataadapter.datafetch.bulk.TableDataArrayRetriever;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.updaters.RecordUpdater;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.util.codegen.CodeGenerator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.dataadapter.rec.json.JsonRecordAdapterUtil.CONVERTIBLE_TO_STRING_CLASSES;

/**
 * Generates a class that efficiently populates JSON ObjectNodes with data from arrays.
 */
public class JsonRecordAdapterGenerator {

    private static final String COMPILED_CLASS_NAME = "JsonRecordAdapter";
    private static final boolean PRINT_CLASS_BODY = true;
    private final String[] colNames;
    private final String[] colTypeNames;
    private final Class<?>[] colTypes;

    public JsonRecordAdapterGenerator(RecordAdapterDescriptor<?> recordAdapterDescriptor) {
        colNames = recordAdapterDescriptor.getColumnNames().toArray(new String[0]);

        final RecordUpdater<?, ?>[] recordUpdaters = recordAdapterDescriptor
                .getColumnAdapters()
                .values()
                .toArray(new RecordUpdater[0]);

        // Create an array of the columns' type names. Sanity check the columns as well.
        colTypes = new Class<?>[recordUpdaters.length];
        colTypeNames = new String[recordUpdaters.length];

        for (int colIdx = 0; colIdx < recordUpdaters.length; colIdx++) {
            try {
                final RecordUpdater<?, ?> recordUpdater = recordUpdaters[colIdx];
                final Class<?> colType = recordUpdater.getSourceType();
                Assert.eqFalse(colType.isAnonymousClass(), "colType.isAnonymousClass()");
                Assert.eqFalse(colType.isLocalClass(), "colType.isLocalClass()");
                String colTypeName = colType.getCanonicalName();
                Assert.neqNull(colTypeName, "colTypeName");

                colTypes[colIdx] = colType;
                colTypeNames[colIdx] = colTypeName;
            } catch (AssertionFailure ex) {
                throw new IllegalArgumentException("Column " + colIdx + " is invalid", ex);
            }
        }
    }

    public Class<? extends BaseJsonRecordAdapter> generate() {
        final String classBody = generateClassBody().build();
        final String desc = "JsonRecordAdapter[" + String.join(", ", colTypeNames);
        if (PRINT_CLASS_BODY) {
            System.out.println("Generated class body for " + desc + ":\n" + classBody);
        }
        // noinspection unchecked
        return (Class<? extends BaseJsonRecordAdapter>) compile(desc, classBody);
    }

    private Class<?> compile(final String desc, final String classBody) {
        try (final QueryPerformanceNugget ignored = QueryPerformanceRecorder.getInstance()
                .getNugget(JsonRecordAdapterGenerator.class.getName() + "Compile: " + desc)) {
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
                "public class $CLASSNAME$ extends [[BASE_JSON_RECORD_ADAPTER_CANONICAL]]", CodeGenerator.block(
                        "",
                        generateConstructor(), "",
                        generatePopulateRecords(), ""));

        g.replace("BASE_JSON_RECORD_ADAPTER_CANONICAL", BaseJsonRecordAdapter.class.getCanonicalName());

        return g.freeze();
    }

    private static String getImportStatements() {
        final Class<?>[] dhClasses = {
                Table.class,
                PartitionedTable.class,
                TableDataArrayRetriever.class,
                PartitionedTableDataArrayRetrieverImpl.class,
                RecordAdapterDescriptor.class,
                RowSequence.class,
                ObjectNode.class,
        };
        final StringBuilder sb = new StringBuilder();
        for (Class<?> clazz : dhClasses) {
            sb.append("import ").append(clazz.getCanonicalName()).append(";\n");
        }

        return sb.toString();
    }

    private CodeGenerator generateConstructor() {
        final CodeGenerator g = CodeGenerator.create(
                "public $CLASSNAME$(Table sourceTable, RecordAdapterDescriptor<ObjectNode> descriptor)",
                CodeGenerator.block(
                        "super(",
                        "descriptor,",
                        "TableDataArrayRetriever.makeDefault(descriptor.getColumnNames(), sourceTable),",
                        CodeGenerator.repeated("columnName", "\"[[COL_NAME]]\"[[TRAILING_COMMA]]"),
                        ");"),
                "",
                "public $CLASSNAME$(PartitionedTable sourceTable, RecordAdapterDescriptor<ObjectNode> descriptor)",
                CodeGenerator.block(
                        "super(",
                        "descriptor,",
                        "new PartitionedTableDataArrayRetrieverImpl(sourceTable, descriptor.getColumnNames()),",
                        CodeGenerator.repeated("columnName2", "\"[[COL_NAME]]\"[[TRAILING_COMMA]]"),
                        ");"));

        for (int i = 0; i < colNames.length;) {
            String colName = colNames[i];
            final CodeGenerator listEntry = g.instantiateNewRepeated("columnName");
            listEntry.replace("COL_NAME", colName);
            final boolean isEndOfList = ++i >= colNames.length;
            listEntry.replace("TRAILING_COMMA", isEndOfList ? "" : ",");
        }

        for (int i = 0; i < colNames.length;) {
            String colName = colNames[i];
            final CodeGenerator listEntry = g.instantiateNewRepeated("columnName2");
            listEntry.replace("COL_NAME", colName);
            final boolean isEndOfList = ++i >= colNames.length;
            listEntry.replace("TRAILING_COMMA", isEndOfList ? "" : ",");
        }

        return g.freeze();
    }

    private CodeGenerator generatePopulateRecords() {
        final CodeGenerator g = CodeGenerator.create(
                "@Override",
                "public void populateRecords(ObjectNode[] recordsArray, Object[] dataArrays)", CodeGenerator.block(
                        "final int nRecords = recordsArray.length;",
                        "int ii;",
                        "",
                        CodeGenerator.repeated("fieldPopulator",
                                "final [[TYPE]][] col[[IDX]] = ([[TYPE]][]) dataArrays[[[IDX]]];",
                                "final String colName[[IDX]] = \"[[COL_NAME]]\";",
                                "for (ii = 0; ii < nRecords; ii++)", CodeGenerator.block(
                                        "final [[TYPE]] val = col[[IDX]][ii];",
                                        "if ([[VAL_NULL_CHECK]])", CodeGenerator.block(
                                                "recordsArray[ii].putNull(colName[[IDX]]);"),
                                        CodeGenerator.samelineBlock("else",
                                                CodeGenerator.optional("__mapped1",
                                                        "final [[MAPPED_TYPE]] mappedVal = [[MAPPER_CODE]];"),
                                                "recordsArray[ii].put(",
                                                "colName[[IDX]],",
                                                CodeGenerator.optional("__mapped2", "mappedVal"),
                                                CodeGenerator.optional("__unmapped", "val"),
                                                ");")),
                                "")));

        for (int colIdx = 0; colIdx < colTypeNames.length; colIdx++) {
            final Class<?> colType = colTypes[colIdx];
            final String colName = colNames[colIdx];
            final String typeName = colTypeNames[colIdx];

            final CodeGenerator fieldPopulator = g.instantiateNewRepeated("fieldPopulator");

            // val mapper result type name + mapper code
            final Pair<String, String> valMapper = getValMapper(colType);
            if (valMapper != null) {
                fieldPopulator.activateOptional("__mapped1");
                fieldPopulator.activateOptional("__mapped2");
                fieldPopulator.replace("MAPPED_TYPE", valMapper.first);
                fieldPopulator.replace("MAPPER_CODE", valMapper.second);
            } else {
                fieldPopulator.activateOptional("__unmapped");
            }

            fieldPopulator.replace("IDX", String.valueOf(colIdx));
            fieldPopulator.replace("COL_NAME", colName);
            fieldPopulator.replace("TYPE", typeName);
            fieldPopulator.replace("VAL_NULL_CHECK", "io.deephaven.function.Basic.isNull(val)");
            fieldPopulator.freeze();
        }

        return g.freeze();
    }

    private static Pair<String, String> getValMapper(@NotNull final Class<?> colType) {
        final boolean isString = String.class.equals(colType);

        final boolean isConvertibleToString = !isString &&
                (CharSequence.class.isAssignableFrom(colType) || CONVERTIBLE_TO_STRING_CLASSES.contains(colType));

        if (isConvertibleToString) {
            return new Pair<>("String", "val.toString()");
        } else if (!isString && !colType.isPrimitive()) {
            // Other reference type are unsupported
            throw new IllegalArgumentException(
                    "Could not update ObjectNode with column of type: " + colType.getCanonicalName());
        } else if (char.class.equals(colType)) {
            return new Pair<>("String", "Character.toString(val)");
        } else {
            // strings and other primitive types are supported directly
            return null;
        }
    }

}
