/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.Pair;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.impl.util.codegen.CodeGenerator;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.text.Indenter;
import io.deephaven.util.type.TypeUtils;
import groovy.json.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.deephaven.engine.table.impl.select.DhFormulaColumn.COLUMN_SUFFIX;

/**
 * A condition filter evaluates a formula against a table.
 */
public class ConditionFilter extends AbstractConditionFilter {

    public static final int CHUNK_SIZE = 4096;
    private Class<?> filterKernelClass = null;
    private List<Pair<String, Class<?>>> usedInputs; // that is columns and special variables
    private String classBody;
    private Filter filter = null;
    private boolean filterValidForCopy = true;

    private ConditionFilter(@NotNull String formula) {
        super(formula, false);
    }

    private ConditionFilter(@NotNull String formula, Map<String, String> renames) {
        super(formula, renames, false);
    }

    public static WhereFilter createConditionFilter(@NotNull String formula, FormulaParserConfiguration parser) {
        switch (parser) {
            case Deephaven:
                return new ConditionFilter(formula);
            case Numba:
                throw new UnsupportedOperationException("Python condition filter should be created from python");
            default:
                throw new UnsupportedOperationException("Unknow parser type " + parser);
        }
    }

    public static WhereFilter createConditionFilter(@NotNull String formula) {
        return createConditionFilter(formula, FormulaParserConfiguration.parser);
    }

    String getClassBodyStr() {
        return classBody;
    }

    public interface FilterKernel<CONTEXT extends FilterKernel.Context> {
        class Context implements io.deephaven.engine.table.Context {
            public final WritableLongChunk<OrderedRowKeys> resultChunk;
            private final io.deephaven.engine.table.Context kernelContext;

            public Context(int maxChunkSize, io.deephaven.engine.table.Context kernelContext) {
                this.resultChunk = WritableLongChunk.makeWritableChunk(maxChunkSize);
                this.kernelContext = kernelContext;
            }

            public Context(int maxChunkSize) {
                this.resultChunk = WritableLongChunk.makeWritableChunk(maxChunkSize);
                this.kernelContext = null;
            }

            public <TYPE extends io.deephaven.engine.table.Context> TYPE getKernelContext() {
                if (kernelContext == null) {
                    throw new IllegalStateException("No kernel context registered");
                }
                // noinspection unchecked
                return (TYPE) kernelContext;
            }

            @Override
            public void close() {
                resultChunk.close();
            }
        }

        CONTEXT getContext(int maxChunkSize);

        LongChunk<OrderedRowKeys> filter(CONTEXT context, LongChunk<OrderedRowKeys> indices,
                Chunk... inputChunks);
    }


    @FunctionalInterface
    public interface ChunkGetter {
        Chunk getChunk(@NotNull Context context, @NotNull RowSequence rowSequence);
    }

    @FunctionalInterface
    public interface ContextGetter {
        Context getContext(int chunkSize);
    }

    interface ChunkGetterWithContext extends ChunkGetter, ContextGetter {
    }

    public static class IndexLookup implements ChunkGetterWithContext {

        protected final RowSet inverted;
        private final RowSequence.Iterator invertedIterator;

        IndexLookup(RowSet fullSet, RowSet selection) {
            this.inverted = fullSet.invert(selection);
            this.invertedIterator = inverted.getRowSequenceIterator();
        }

        static class Context implements io.deephaven.engine.table.Context {
            private final WritableLongChunk<OrderedRowKeys> chunk;

            Context(int chunkSize) {
                this.chunk = WritableLongChunk.makeWritableChunk(chunkSize);
            }

            @Override
            public void close() {
                chunk.close();
            }
        }

        public Context getContext(int chunkSize) {
            return new Context(chunkSize);
        }

        @Override
        public Chunk getChunk(@NotNull io.deephaven.engine.table.Context context,
                @NotNull RowSequence rowSequence) {
            final WritableLongChunk<OrderedRowKeys> wlc = ((Context) context).chunk;
            final RowSequence valuesForChunk = invertedIterator.getNextRowSequenceWithLength(rowSequence.size());
            valuesForChunk.fillRowKeyChunk(wlc);
            return wlc;
        }
    }

    public static final class ColumnILookup extends IndexLookup {
        ColumnILookup(RowSet fullSet, RowSet selection) {
            super(fullSet, selection);
        }

        static class IntegerContext extends IndexLookup.Context {
            private final WritableIntChunk<Any> intChunk;

            private IntegerContext(int chunkSize) {
                super(chunkSize);
                intChunk = WritableIntChunk.makeWritableChunk(chunkSize);
            }


            @Override
            public void close() {
                super.close();
                intChunk.close();
            }
        }

        @Override
        public Context getContext(int chunkSize) {
            return new IntegerContext(chunkSize);
        }

        @Override
        public Chunk getChunk(@NotNull io.deephaven.engine.table.Context context,
                @NotNull RowSequence rowSequence) {
            final LongChunk lc = super.getChunk(context, rowSequence).asLongChunk();
            final WritableIntChunk<Any> wic = ((IntegerContext) context).intChunk;
            wic.setSize(lc.size());
            for (int ii = 0; ii < lc.size(); ++ii) {
                wic.set(ii, (int) lc.get(ii));
            }
            return wic;
        }
    }

    public static abstract class IndexCount implements ChunkGetterWithContext {
        private final ChunkType chunkType;

        IndexCount(ChunkType chunkType) {
            this.chunkType = chunkType;
        }

        class Context implements io.deephaven.engine.table.Context {
            private final WritableChunk chunk;
            long pos = 0;

            Context(int chunkSize) {
                this.chunk = chunkType.makeWritableChunk(chunkSize);
            }

            @Override
            public void close() {
                chunk.close();
            }
        }

        public Context getContext(int chunkSize) {
            return new Context(chunkSize);
        }
    }


    public static final class ColumnIICount extends IndexCount {

        ColumnIICount() {
            super(ChunkType.Long);
        }

        @Override
        public Chunk getChunk(@NotNull io.deephaven.engine.table.Context context,
                @NotNull RowSequence rowSequence) {
            final Context ctx = (Context) context;
            final WritableLongChunk wlc = ctx.chunk.asWritableLongChunk();
            for (int i = 0; i < rowSequence.size(); i++) {
                wlc.set(i, ctx.pos++);
            }
            return wlc;
        }
    }

    public static final class ColumnICount extends IndexCount {

        ColumnICount() {
            super(ChunkType.Int);
        }

        @Override
        public Chunk getChunk(@NotNull io.deephaven.engine.table.Context context,
                @NotNull RowSequence rowSequence) {
            final Context ctx = (Context) context;
            final WritableIntChunk wic = ctx.chunk.asWritableIntChunk();
            for (int ii = 0; ii < rowSequence.size(); ii++) {
                wic.set(ii, (int) ctx.pos++);
            }
            return wic;
        }
    }

    static class RowSequenceChunkGetter implements ChunkGetterWithContext {
        private static final Context nullContext = new Context() {};

        @Override
        public Chunk getChunk(@NotNull Context context, @NotNull RowSequence rowSequence) {
            return rowSequence.asRowKeyChunk();
        }

        @Override
        public Context getContext(int chunkSize) {
            return nullContext;
        }
    }

    public static class ChunkFilter implements Filter {

        private final FilterKernel filterKernel;
        private final String[] columnNames;
        private final int chunkSize;

        public ChunkFilter(FilterKernel filterKernel, String[] columnNames, int chunkSize) {
            this.filterKernel = filterKernel;
            this.columnNames = columnNames;
            this.chunkSize = chunkSize;
        }

        private SharedContext populateChunkGettersAndContexts(
                final RowSet selection, final RowSet fullSet, final Table table, final boolean usePrev,
                final ChunkGetter[] chunkGetters, final Context[] sourceContexts) {
            final SharedContext sharedContext = (columnNames.length > 1) ? SharedContext.makeSharedContext() : null;
            for (int i = 0; i < columnNames.length; i++) {
                final String columnName = columnNames[i];
                final ChunkGetterWithContext chunkGetterWithContext;
                switch (columnName) {
                    case "i":
                        chunkGetterWithContext =
                                (selection == fullSet ? new ColumnICount() : new ColumnILookup(fullSet, selection));
                        chunkGetters[i] = chunkGetterWithContext;
                        sourceContexts[i] = chunkGetterWithContext.getContext(chunkSize);
                        break;
                    case "ii":
                        chunkGetterWithContext =
                                (selection == fullSet ? new ColumnIICount() : new IndexLookup(fullSet, selection));
                        chunkGetters[i] = chunkGetterWithContext;
                        sourceContexts[i] = chunkGetterWithContext.getContext(chunkSize);
                        break;
                    case "k":
                        chunkGetterWithContext = new RowSequenceChunkGetter();
                        chunkGetters[i] = chunkGetterWithContext;
                        sourceContexts[i] = chunkGetterWithContext.getContext(chunkSize);
                        break;
                    default: {
                        final ColumnSource columnSource = table.getColumnSource(columnName);
                        chunkGetters[i] = usePrev
                                ? (context, RowSequence) -> columnSource.getPrevChunk((ColumnSource.GetContext) context,
                                        RowSequence)
                                : (context, RowSequence) -> columnSource.getChunk((ColumnSource.GetContext) context,
                                        RowSequence);
                        sourceContexts[i] = columnSource.makeGetContext(chunkSize, sharedContext);
                    }
                }
            }
            return sharedContext;
        }

        @Override
        public WritableRowSet filter(final RowSet selection, final RowSet fullSet, final Table table,
                final boolean usePrev,
                String formula, final QueryScopeParam... params) {
            try (final FilterKernel.Context context = filterKernel.getContext(chunkSize);
                    final RowSequence.Iterator rsIterator = selection.getRowSequenceIterator()) {
                final ChunkGetter[] chunkGetters = new ChunkGetter[columnNames.length];
                final Context[] sourceContexts = new Context[columnNames.length];
                final SharedContext sharedContext = populateChunkGettersAndContexts(selection, fullSet, table, usePrev,
                        chunkGetters, sourceContexts);
                final RowSetBuilderSequential resultBuilder = RowSetFactory.builderSequential();
                final Chunk[] inputChunks = new Chunk[columnNames.length];
                while (rsIterator.hasMore()) {
                    final RowSequence currentChunkRowSequence = rsIterator.getNextRowSequenceWithLength(chunkSize);
                    for (int i = 0; i < chunkGetters.length; i++) {
                        final ChunkGetter chunkFiller = chunkGetters[i];
                        inputChunks[i] = chunkFiller.getChunk(sourceContexts[i], currentChunkRowSequence);
                    }
                    if (sharedContext != null) {
                        sharedContext.reset();
                    }
                    try {
                        // noinspection unchecked
                        final LongChunk<OrderedRowKeys> matchedIndices =
                                filterKernel.filter(context, currentChunkRowSequence.asRowKeyChunk(), inputChunks);
                        resultBuilder.appendOrderedRowKeysChunk(matchedIndices);
                    } catch (Exception e) {
                        throw new FormulaEvaluationException(e.getClass().getName() + " encountered in filter={ "
                                + StringEscapeUtils.escapeJava(truncateLongFormula(formula)) + " }", e);
                    }
                }
                SafeCloseable.closeAll(sourceContexts);
                if (sharedContext != null) {
                    sharedContext.close();
                }
                return resultBuilder.build();
            }
        }
    }

    private static String toTitleCase(String input) {
        return Character.toUpperCase(input.charAt(0)) + input.substring(1);
    }

    @Override
    protected void generateFilterCode(TableDefinition tableDefinition, DateTimeUtils.Result timeConversionResult,
            QueryLanguageParser.Result result) {
        final StringBuilder classBody = getClassBody(tableDefinition, timeConversionResult, result);
        if (classBody == null)
            return;
        final QueryPerformanceNugget nugget = QueryPerformanceRecorder.getInstance().getNugget("Compile:" + formula);
        try {
            final List<Class<?>> paramClasses = new ArrayList<>();
            final Consumer<Class<?>> addParamClass = (cls) -> {
                if (cls != null) {
                    paramClasses.add(cls);
                }
            };
            for (String usedColumn : usedColumns) {
                usedColumn = outerToInnerNames.getOrDefault(usedColumn, usedColumn);
                final ColumnDefinition<?> column = tableDefinition.getColumn(usedColumn);
                addParamClass.accept(column.getDataType());
                addParamClass.accept(column.getComponentType());
            }
            for (String usedColumn : usedColumnArrays) {
                usedColumn = outerToInnerNames.getOrDefault(usedColumn, usedColumn);
                final ColumnDefinition<?> column = tableDefinition.getColumn(usedColumn);
                addParamClass.accept(column.getDataType());
                addParamClass.accept(column.getComponentType());
            }
            for (final QueryScopeParam<?> param : params) {
                addParamClass.accept(QueryScopeParamTypeUtil.getDeclaredClass(param.getValue()));
            }

            filterKernelClass = ExecutionContext.getContext().getQueryCompiler()
                    .compile("GeneratedFilterKernel", this.classBody = classBody.toString(),
                            QueryCompiler.FORMULA_PREFIX, QueryScopeParamTypeUtil.expandParameterClasses(paramClasses));
        } finally {
            nugget.done();
        }
    }

    @Nullable
    private StringBuilder getClassBody(TableDefinition tableDefinition, DateTimeUtils.Result timeConversionResult,
            QueryLanguageParser.Result result) {
        if (filterKernelClass != null) {
            return null;
        }
        usedInputs = new ArrayList<>();
        for (String usedColumn : usedColumns) {
            final String innerName = outerToInnerNames.getOrDefault(usedColumn, usedColumn);
            final ColumnDefinition<?> column = tableDefinition.getColumn(innerName);
            final Class<?> columnType = column.getDataType();
            usedInputs.add(new Pair<>(usedColumn, columnType));
        }
        if (usesI) {
            usedInputs.add(new Pair<>("i", int.class));
        }
        if (usesII) {
            usedInputs.add(new Pair<>("ii", long.class));
        }
        if (usesK) {
            usedInputs.add(new Pair<>("k", long.class));
        }
        final StringBuilder classBody = new StringBuilder();
        classBody
                .append(CodeGenerator
                        .create(ExecutionContext.getContext().getQueryLibrary().getImportStrings().toArray()).build())
                .append(
                        "\n\npublic class $CLASSNAME$ implements ")
                .append(FilterKernel.class.getCanonicalName()).append("<FilterKernel.Context>{\n");
        classBody.append("\n").append(timeConversionResult.getInstanceVariablesString()).append("\n");
        final Indenter indenter = new Indenter();
        for (QueryScopeParam<?> param : params) {
            /*
             * @formatter:off
             * adding context param fields like:
             *
             * final int p1;
             * final float p2;
             * final String p3;
             * @formatter:on
             */
            classBody.append(indenter).append("private final ")
                    .append(QueryScopeParamTypeUtil.getPrimitiveTypeNameIfAvailable(param.getValue()))
                    .append(" ").append(param.getName()).append(";\n");
        }
        if (!usedColumnArrays.isEmpty()) {
            classBody.append(indenter).append("// Array Column Variables\n");
            for (String columnName : usedColumnArrays) {
                final String innerColumnName = outerToInnerNames.getOrDefault(columnName, columnName);
                final ColumnDefinition<?> column = tableDefinition.getColumn(innerColumnName);
                if (column == null) {
                    throw new RuntimeException("Column \"" + innerColumnName + "\" doesn't exist in this table");
                }
                final Class<?> dataType = column.getDataType();
                final Class<?> columnType = DhFormulaColumn.getVectorType(dataType);

                /*
                 * Adding array column fields.
                 */
                classBody.append(indenter).append("private final ").append(columnType.getCanonicalName())
                        .append(TypeUtils.isConvertibleToPrimitive(dataType) ? ""
                                : "<" + dataType.getCanonicalName() + ">")
                        .append(" ").append(columnName).append(COLUMN_SUFFIX).append(";\n");
            }
            classBody.append("\n");
        }

        classBody.append("\n").append(indenter)
                .append("public $CLASSNAME$(Table __table, RowSet __fullSet, QueryScopeParam... __params) {\n");
        indenter.increaseLevel();
        for (int i = 0; i < params.length; i++) {
            final QueryScopeParam<?> param = params[i];
            /*
             * @formatter:off
             * Initializing context parameters:
             *
             * this.p1 = (Integer) __params[0].getValue();
             * this.p2 = (Float) __params[1].getValue();
             * this.p3 = (String) __params[2].getValue();
             * @formatter:on
             */
            final String name = param.getName();
            classBody.append(indenter).append("this.").append(name).append(" = (")
                    .append(QueryScopeParamTypeUtil.getDeclaredTypeName(param.getValue()))
                    .append(") __params[").append(i).append("].getValue();\n");
        }

        if (!usedColumnArrays.isEmpty()) {
            classBody.append("\n");
            classBody.append(indenter).append("// Array Column Variables\n");
            for (String columnName : usedColumnArrays) {
                final String innerColumnName = outerToInnerNames.getOrDefault(columnName, columnName);
                final ColumnDefinition<?> column = tableDefinition.getColumn(innerColumnName);
                if (column == null) {
                    throw new RuntimeException("Column \"" + innerColumnName + "\" doesn't exist in this table");
                }
                final Class<?> dataType = column.getDataType();
                final Class<?> columnType = DhFormulaColumn.getVectorType(dataType);

                final String arrayType = columnType.getCanonicalName().replace(
                        "io.deephaven.vector",
                        "io.deephaven.engine.table.impl.vector") + "ColumnWrapper";

                /*
                 * Adding array column fields.
                 */
                classBody.append(indenter).append(columnName).append(COLUMN_SUFFIX).append(" = new ")
                        .append(arrayType).append("(__table.getColumnSource(\"").append(innerColumnName)
                        .append("\"), __fullSet);\n");
            }
        }

        indenter.decreaseLevel();

        indenter.indent(classBody, "}\n" +
                "@Override\n" +
                "public Context getContext(int __maxChunkSize) {\n" +
                "    return new Context(__maxChunkSize);\n" +
                "}\n" +
                "\n" +
                "@Override\n" +
                "public LongChunk<OrderedRowKeys> filter(Context __context, LongChunk<OrderedRowKeys> __indices, Chunk... __inputChunks) {\n");
        indenter.increaseLevel();
        for (int i = 0; i < usedInputs.size(); i++) {
            final Class<?> columnType = usedInputs.get(i).second;
            final String chunkType;
            if (columnType.isPrimitive() && columnType != boolean.class) {
                chunkType = toTitleCase(columnType.getSimpleName()) + "Chunk";
            } else {
                // TODO: Reinterpret Boolean and DateTime to byte and long
                chunkType = "ObjectChunk";
            }
            classBody.append(indenter).append("final ").append(chunkType).append(" __columnChunk").append(i)
                    .append(" = __inputChunks[").append(i).append("].as").append(chunkType).append("();\n");
        }
        indenter.indent(classBody, "final int __size = __indices.size();\n" +
                "__context.resultChunk.setSize(0);\n" +
                "for (int __my_i__ = 0; __my_i__ < __size; __my_i__++) {\n");
        indenter.increaseLevel();
        for (int i = 0; i < usedInputs.size(); i++) {
            final Pair<String, Class<?>> usedInput = usedInputs.get(i);
            final Class<?> columnType = usedInput.second;
            final String canonicalName = columnType.getCanonicalName();
            classBody.append(indenter).append("final ").append(canonicalName).append(" ").append(usedInput.first)
                    .append(" =  (").append(canonicalName).append(")__columnChunk").append(i)
                    .append(".get(__my_i__);\n");
        }
        classBody.append(
                "            if (").append(result.getConvertedExpression()).append(") {\n" +
                        "                __context.resultChunk.add(__indices.get(__my_i__));\n" +
                        "            }\n" +
                        "        }\n" +
                        "        return __context.resultChunk;\n" +
                        "    }\n" +
                        "}");
        return classBody;
    }

    @Override
    protected Filter getFilter(Table table, RowSet fullSet)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        if (filter == null) {
            final FilterKernel<?> filterKernel = (FilterKernel<?>) filterKernelClass
                    .getConstructor(Table.class, RowSet.class, QueryScopeParam[].class)
                    .newInstance(table, fullSet, (Object) params);
            final String[] columnNames = usedInputs.stream()
                    .map(p -> outerToInnerNames.getOrDefault(p.first, p.first))
                    .toArray(String[]::new);
            filter = new ChunkFilter(filterKernel, columnNames, CHUNK_SIZE);
            // note this filter is not valid for use in other contexts, as it captures references from the source table
            filterValidForCopy = false;
        }
        return filter;
    }

    @Override
    protected void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public ConditionFilter copy() {
        final ConditionFilter copy = new ConditionFilter(formula, outerToInnerNames);
        onCopy(copy);
        if (initialized) {
            copy.filterKernelClass = filterKernelClass;
            copy.usedInputs = usedInputs;
            copy.classBody = classBody;
            if (filterValidForCopy) {
                copy.filter = filter;
            }
        }
        return copy;
    }

    @Override
    public ConditionFilter renameFilter(Map<String, String> renames) {
        return new ConditionFilter(formula, renames);
    }
}
