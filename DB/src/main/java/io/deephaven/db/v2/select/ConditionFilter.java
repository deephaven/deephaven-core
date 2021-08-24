/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.base.Pair;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.lang.DBLanguageParser;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.select.Param;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.QueryPerformanceNugget;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.Index.SequentialBuilder;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.text.Indenter;
import io.deephaven.util.type.TypeUtils;
import groovy.json.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.deephaven.db.v2.select.DhFormulaColumn.COLUMN_SUFFIX;

/**
 * A condition filter evaluates a formula against a table.
 */
public class ConditionFilter extends AbstractConditionFilter {

    public static final int CHUNK_SIZE = 4096;
    private Class<?> filterKernelClass = null;
    private List<Pair<String, Class>> usedInputs; // that is columns and special variables
    private String classBody;
    private Filter filter = null;

    private ConditionFilter(@NotNull String formula) {
        super(formula, false);
    }

    private ConditionFilter(@NotNull String formula, Map<String, String> renames) {
        super(formula, renames, false);
    }

    public static SelectFilter createConditionFilter(@NotNull String formula,
        FormulaParserConfiguration parser) {
        switch (parser) {
            case Deephaven:
                return new ConditionFilter(formula);
            case Numba:
                throw new UnsupportedOperationException(
                    "Python condition filter should be created from python");
            default:
                throw new UnsupportedOperationException("Unknow parser type " + parser);
        }
    }

    public static SelectFilter createConditionFilter(@NotNull String formula) {
        return createConditionFilter(formula, FormulaParserConfiguration.parser);
    }

    String getClassBodyStr() {
        return classBody;
    }

    public interface FilterKernel<CONTEXT extends FilterKernel.Context> {

        class Context implements io.deephaven.db.v2.sources.chunk.Context {
            public final WritableLongChunk<OrderedKeyIndices> resultChunk;

            public Context(int maxChunkSize) {
                resultChunk = WritableLongChunk.makeWritableChunk(maxChunkSize);
            }

            @Override
            public void close() {
                resultChunk.close();
            }
        }

        CONTEXT getContext(int maxChunkSize);

        LongChunk<OrderedKeyIndices> filter(CONTEXT context, LongChunk<OrderedKeyIndices> indices,
            Chunk... inputChunks);
    }


    @FunctionalInterface
    public interface ChunkGetter {
        Chunk getChunk(@NotNull Context context, @NotNull OrderedKeys orderedKeys);
    }

    @FunctionalInterface
    public interface ContextGetter {
        Context getContext(int chunkSize);
    }

    interface ChunkGetterWithContext extends ChunkGetter, ContextGetter {
    }

    public static class IndexLookup implements ChunkGetterWithContext {

        protected final Index inverted;
        private final OrderedKeys.Iterator invertedIterator;

        IndexLookup(Index fullSet, Index selection) {
            this.inverted = fullSet.invert(selection);
            this.invertedIterator = inverted.getOrderedKeysIterator();
        }

        static class Context implements io.deephaven.db.v2.sources.chunk.Context {
            private final WritableLongChunk<OrderedKeyIndices> chunk;

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
        public Chunk getChunk(@NotNull io.deephaven.db.v2.sources.chunk.Context context,
            @NotNull OrderedKeys orderedKeys) {
            final WritableLongChunk<OrderedKeyIndices> wlc = ((Context) context).chunk;
            final OrderedKeys valuesForChunk =
                invertedIterator.getNextOrderedKeysWithLength(orderedKeys.size());
            valuesForChunk.fillKeyIndicesChunk(wlc);
            return wlc;
        }
    }

    public static final class ColumnILookup extends IndexLookup {
        ColumnILookup(Index fullSet, Index selection) {
            super(fullSet, selection);
        }

        static class IntegerContext extends IndexLookup.Context {
            private final WritableIntChunk<Attributes.Any> intChunk;

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
        public Chunk getChunk(@NotNull io.deephaven.db.v2.sources.chunk.Context context,
            @NotNull OrderedKeys orderedKeys) {
            final LongChunk lc = super.getChunk(context, orderedKeys).asLongChunk();
            final WritableIntChunk<Attributes.Any> wic = ((IntegerContext) context).intChunk;
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

        class Context implements io.deephaven.db.v2.sources.chunk.Context {
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
        public Chunk getChunk(@NotNull io.deephaven.db.v2.sources.chunk.Context context,
            @NotNull OrderedKeys orderedKeys) {
            final Context ctx = (Context) context;
            final WritableLongChunk wlc = ctx.chunk.asWritableLongChunk();
            for (int i = 0; i < orderedKeys.size(); i++) {
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
        public Chunk getChunk(@NotNull io.deephaven.db.v2.sources.chunk.Context context,
            @NotNull OrderedKeys orderedKeys) {
            final Context ctx = (Context) context;
            final WritableIntChunk wic = ctx.chunk.asWritableIntChunk();
            for (int ii = 0; ii < orderedKeys.size(); ii++) {
                wic.set(ii, (int) ctx.pos++);
            }
            return wic;
        }
    }

    static class OrderedKeysChunkGetter implements ChunkGetterWithContext {
        private static final Context nullContext = new Context() {};

        @Override
        public Chunk getChunk(@NotNull Context context, @NotNull OrderedKeys orderedKeys) {
            return orderedKeys.asKeyIndicesChunk();
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
            final Index selection, final Index fullSet, final Table table, final boolean usePrev,
            final ChunkGetter[] chunkGetters, final Context[] sourceContexts) {
            final SharedContext sharedContext =
                (columnNames.length > 1) ? SharedContext.makeSharedContext() : null;
            for (int i = 0; i < columnNames.length; i++) {
                final String columnName = columnNames[i];
                final ChunkGetterWithContext chunkGetterWithContext;
                switch (columnName) {
                    case "i":
                        chunkGetterWithContext = (selection == fullSet ? new ColumnICount()
                            : new ColumnILookup(fullSet, selection));
                        chunkGetters[i] = chunkGetterWithContext;
                        sourceContexts[i] = chunkGetterWithContext.getContext(chunkSize);
                        break;
                    case "ii":
                        chunkGetterWithContext = (selection == fullSet ? new ColumnIICount()
                            : new IndexLookup(fullSet, selection));
                        chunkGetters[i] = chunkGetterWithContext;
                        sourceContexts[i] = chunkGetterWithContext.getContext(chunkSize);
                        break;
                    case "k":
                        chunkGetterWithContext = new OrderedKeysChunkGetter();
                        chunkGetters[i] = chunkGetterWithContext;
                        sourceContexts[i] = chunkGetterWithContext.getContext(chunkSize);
                        break;
                    default: {
                        final ColumnSource columnSource = table.getColumnSource(columnName);
                        chunkGetters[i] = usePrev
                            ? (context, orderedKeys) -> columnSource
                                .getPrevChunk((ColumnSource.GetContext) context, orderedKeys)
                            : (context, orderedKeys) -> columnSource
                                .getChunk((ColumnSource.GetContext) context, orderedKeys);
                        sourceContexts[i] = columnSource.makeGetContext(chunkSize, sharedContext);
                    }
                }
            }
            return sharedContext;
        }

        @Override
        public Index filter(final Index selection, final Index fullSet, final Table table,
            final boolean usePrev, String formula, final Param... params) {
            try (final FilterKernel.Context context = filterKernel.getContext(chunkSize);
                final OrderedKeys.Iterator okIterator = selection.getOrderedKeysIterator()) {
                final ChunkGetter[] chunkGetters = new ChunkGetter[columnNames.length];
                final Context sourceContexts[] = new Context[columnNames.length];
                final SharedContext sharedContext = populateChunkGettersAndContexts(selection,
                    fullSet, table, usePrev, chunkGetters, sourceContexts);
                final SequentialBuilder resultBuilder = Index.FACTORY.getSequentialBuilder();
                final Chunk inputChunks[] = new Chunk[columnNames.length];
                while (okIterator.hasMore()) {
                    final OrderedKeys currentChunkOrderedKeys =
                        okIterator.getNextOrderedKeysWithLength(chunkSize);
                    for (int i = 0; i < chunkGetters.length; i++) {
                        final ChunkGetter chunkFiller = chunkGetters[i];
                        inputChunks[i] =
                            chunkFiller.getChunk(sourceContexts[i], currentChunkOrderedKeys);
                    }
                    if (sharedContext != null) {
                        sharedContext.reset();
                    }
                    try {
                        final LongChunk<OrderedKeyIndices> matchedIndices = filterKernel.filter(
                            context, currentChunkOrderedKeys.asKeyIndicesChunk(), inputChunks);
                        resultBuilder.appendOrderedKeyIndicesChunk(matchedIndices);
                    } catch (Exception e) {
                        throw new FormulaEvaluationException(
                            e.getClass().getName() + " encountered in filter={ "
                                + StringEscapeUtils.escapeJava(truncateLongFormula(formula)) + " }",
                            e);
                    }
                }
                SafeCloseable.closeArray(sourceContexts);
                if (sharedContext != null) {
                    sharedContext.close();
                }
                return resultBuilder.getIndex();
            }
        }


    }

    private static String toTitleCase(String input) {
        return Character.toUpperCase(input.charAt(0)) + input.substring(1);
    }

    @Override
    protected void generateFilterCode(TableDefinition tableDefinition,
        DBTimeUtils.Result timeConversionResult, DBLanguageParser.Result result)
        throws MalformedURLException, ClassNotFoundException {
        final StringBuilder classBody = getClassBody(tableDefinition, timeConversionResult, result);
        if (classBody == null)
            return;
        final QueryPerformanceNugget nugget =
            QueryPerformanceRecorder.getInstance().getNugget("Compile:" + formula);
        try {
            final List<Class<?>> paramClasses = new ArrayList<>();
            final Consumer<Class<?>> addParamClass = (cls) -> {
                if (cls != null) {
                    paramClasses.add(cls);
                }
            };
            for (final String usedColumn : usedColumns) {
                final ColumnDefinition<?> column = tableDefinition.getColumn(usedColumn);
                addParamClass.accept(column.getDataType());
                addParamClass.accept(column.getComponentType());
            }
            for (final String usedColumn : usedColumnArrays) {
                final ColumnDefinition<?> column = tableDefinition.getColumn(usedColumn);
                addParamClass.accept(column.getDataType());
                addParamClass.accept(column.getComponentType());
            }
            for (final Param<?> param : params) {
                addParamClass.accept(param.getDeclaredType());
            }

            filterKernelClass = CompilerTools.compile("GeneratedFilterKernel",
                this.classBody = classBody.toString(), CompilerTools.FORMULA_PREFIX,
                Param.expandParameterClasses(paramClasses));
        } finally {
            nugget.done();
        }
    }

    @Nullable
    private StringBuilder getClassBody(TableDefinition tableDefinition,
        DBTimeUtils.Result timeConversionResult, DBLanguageParser.Result result) {
        if (filterKernelClass != null) {
            return null;
        }
        usedInputs = new ArrayList<>();
        for (String usedColumn : usedColumns) {

            final ColumnDefinition column = tableDefinition.getColumn(usedColumn);
            final Class columnType = column.getDataType();
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
        classBody.append(QueryLibrary.getImportStatement().build()).append(
            "\n\n" +
                "public class $CLASSNAME$ implements ")
            .append(FilterKernel.class.getCanonicalName()).append("<FilterKernel.Context>{\n");
        classBody.append("\n").append(timeConversionResult.getInstanceVariablesString())
            .append("\n");
        final Indenter indenter = new Indenter();
        for (Param param : params) {
            /*
             * adding context param fields like: "            final int p1;\n" +
             * "            final float p2;\n" + "            final String p3;\n" +
             */
            classBody.append(indenter).append("private final ")
                .append(param.getPrimitiveTypeNameIfAvailable()).append(" ").append(param.getName())
                .append(";\n");
        }
        if (!usedColumnArrays.isEmpty()) {
            classBody.append(indenter).append("// Array Column Variables\n");
            for (String columnName : usedColumnArrays) {
                final ColumnDefinition column = tableDefinition.getColumn(columnName);
                if (column == null) {
                    throw new RuntimeException(
                        "Column \"" + columnName + "\" doesn't exist in this table");
                }
                final Class dataType = column.getDataType();
                final Class columnType = DhFormulaColumn.getDbArrayType(dataType);

                /*
                 * Adding array column fields.
                 */
                classBody.append(indenter).append("private final ")
                    .append(columnType.getCanonicalName())
                    .append(TypeUtils.isConvertibleToPrimitive(dataType) ? ""
                        : "<" + dataType.getCanonicalName() + ">")
                    .append(" ").append(column.getName()).append(COLUMN_SUFFIX).append(";\n");
            }
            classBody.append("\n");
        }

        classBody.append("\n").append(indenter)
            .append("public $CLASSNAME$(Table table, Index fullSet, Param... params) {\n");
        indenter.increaseLevel();
        for (int i = 0; i < params.length; i++) {
            final Param param = params[i];
            /*
             * Initializing context parameters this.p1 = (Integer) params[0].getValue(); this.p2 =
             * (Float) params[1].getValue(); this.p3 = (String) params[2].getValue();
             */
            final String name = param.getName();
            classBody.append(indenter).append("this.").append(name).append(" = (")
                .append(param.getDeclaredTypeName()).append(") params[").append(i)
                .append("].getValue();\n");
        }

        if (!usedColumnArrays.isEmpty()) {
            classBody.append("\n");
            classBody.append(indenter).append("// Array Column Variables\n");
            for (String columnName : usedColumnArrays) {
                final ColumnDefinition column = tableDefinition.getColumn(columnName);
                if (column == null) {
                    throw new RuntimeException(
                        "Column \"" + columnName + "\" doesn't exist in this table");
                }
                final Class dataType = column.getDataType();
                final Class columnType = DhFormulaColumn.getDbArrayType(dataType);

                final String arrayType = columnType.getCanonicalName().replace(
                    "io.deephaven.db.tables.dbarrays",
                    "io.deephaven.db.v2.dbarrays") + "ColumnWrapper";

                /*
                 * Adding array column fields.
                 */
                classBody.append(indenter).append(column.getName()).append(COLUMN_SUFFIX)
                    .append(" = new ").append(arrayType).append("(table.getColumnSource(\"")
                    .append(columnName).append("\"), fullSet);\n");
            }
        }

        indenter.decreaseLevel();

        indenter.indent(classBody, "}\n" +
            "@Override\n" +
            "public Context getContext(int maxChunkSize) {\n" +
            "    return new Context(maxChunkSize);\n" +
            "}\n" +
            "\n" +
            "@Override\n" +
            "public LongChunk<OrderedKeyIndices> filter(Context context, LongChunk<OrderedKeyIndices> indices, Chunk... inputChunks) {\n");
        indenter.increaseLevel();
        for (int i = 0; i < usedInputs.size(); i++) {
            final Class columnType = usedInputs.get(i).second;
            final String chunkType;
            if (columnType.isPrimitive() && columnType != boolean.class) {
                chunkType = toTitleCase(columnType.getSimpleName()) + "Chunk";
            } else {
                // TODO: Reinterpret Boolean and DBDateTime to byte and long
                chunkType = "ObjectChunk";
            }
            classBody.append(indenter).append("final ").append(chunkType).append(" __columnChunk")
                .append(i).append(" = inputChunks[").append(i).append("].as").append(chunkType)
                .append("();\n");
        }
        indenter.indent(classBody, "final int size = indices.size();\n" +
            "context.resultChunk.setSize(0);\n" +
            "for (int __my_i__ = 0; __my_i__ < size; __my_i__++) {\n");
        indenter.increaseLevel();
        for (int i = 0; i < usedInputs.size(); i++) {
            final Pair<String, Class> usedInput = usedInputs.get(i);
            final Class columnType = usedInput.second;
            final String canonicalName = columnType.getCanonicalName();
            classBody.append(indenter).append("final ").append(canonicalName).append(" ")
                .append(usedInput.first).append(" =  (").append(canonicalName)
                .append(")__columnChunk").append(i).append(".get(__my_i__);\n");
        }
        classBody.append(
            "            if (").append(result.getConvertedExpression()).append(") {\n" +
                "                context.resultChunk.add(indices.get(__my_i__));\n" +
                "            }\n" +
                "        }\n" +
                "        return context.resultChunk;\n" +
                "    }\n" +
                "}");
        return classBody;
    }

    @Override
    protected Filter getFilter(Table table, Index fullSet) throws InstantiationException,
        IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        if (filter != null) {
            return filter;
        }
        final FilterKernel filterKernel =
            (FilterKernel) filterKernelClass.getConstructor(Table.class, Index.class, Param[].class)
                .newInstance(table, fullSet, (Object) params);
        final String[] columnNames = usedInputs.stream().map(p -> p.first).toArray(String[]::new);
        return new ChunkFilter(filterKernel, columnNames, CHUNK_SIZE);
    }

    @Override
    protected void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public ConditionFilter copy() {
        return new ConditionFilter(formula, outerToInnerNames);
    }

    @Override
    public ConditionFilter renameFilter(Map<String, String> renames) {
        return new ConditionFilter(formula, renames);
    }
}
