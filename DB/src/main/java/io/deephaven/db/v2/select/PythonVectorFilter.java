package io.deephaven.db.v2.select;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.lang.DBLanguageParser;
import io.deephaven.db.tables.utils.DBTimeUtils.Result;
import io.deephaven.db.v2.select.ConditionFilter.ChunkFilter;
import io.deephaven.db.v2.select.ConditionFilter.FilterKernel;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.jpy.JpyModule;
import org.jetbrains.annotations.NotNull;
import org.jpy.*;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

/**
 * A {@link AbstractConditionFilter} which transfers primitive data to python in bulk,
 * which is sent as numba arrays into a bulk `@vectorize`d function,
 * where the entire loop is processed in native C.
 * <p>
 * For very complex expressions, we may want to consider using @guvectorize and cuda mode,
 * which would be able to process large chunks of data in parallel, in C.
 */
public class PythonVectorFilter extends AbstractConditionFilter {

    private PyDictWrapper vectorized;
    private LinkedHashMap<String, String> usedCols;

    protected PythonVectorFilter(@NotNull String formula) {
        super(formula, false);
    }

    private PythonVectorFilter(@NotNull String formula, Map<String, String> renames) {
        super(formula, renames, false);
    }

    @Override
    public synchronized void init(TableDefinition tableDefinition) {

        if (initialized) {
            return;
        }
        initialized = true;
        vectorized = NumbaCompileTools.compileFormula(formula, NumbaCompileTools.getExpandedColumnNames(tableDefinition));
        if (!vectorized.get("return_type").toString().equals("bool")) {
            throw new FormulaCompilationException("Expression " + formula + " return type is not bool but " + vectorized.get("return_type"));
        }
        usedCols = NumbaCompileTools.findUsedColumns(vectorized, NumbaCompileTools.getExpandedColumnNames(tableDefinition));
    }

    @Override
    public boolean isRefreshing() {
        return false;
    }

    @Override
    public boolean canMemoize() {
        return false;
    }

    @Override
    protected void generateFilterCode(
            TableDefinition tableDefinition, Result timeConversionResult, DBLanguageParser.Result result
    ) {
        throw new UnsupportedOperationException("Python filter does not generate java code");
    }

    @Override
    protected Filter getFilter(Table table, Index fullSet) {
        final Map<String, ColumnDefinition> columnDefMap = NumbaCompileTools.getExpandedColumnNames(table.getDefinition());
        final Class[] usedColumnsTypes = usedCols.keySet().stream().map(name->columnDefMap.get(name).getDataType()).toArray(Class[]::new);
        return new ChunkFilter(new PythonVectorKernel(vectorized.get("fun"), usedCols, usedColumnsTypes), usedCols.keySet().toArray(ZERO_LENGTH_STRING_ARRAY), ConditionFilter.CHUNK_SIZE);
    }

    @Override
    public AbstractConditionFilter copy() {
        // Hm. should send the already-compiled function? ...probably
        return new PythonVectorFilter(formula, outerToInnerNames);
    }

    @Override
    public AbstractConditionFilter renameFilter(Map<String, String> renames) {
        // This one probably shouldn't send the already-compiled function...
        return new PythonVectorFilter(formula, renames);
    }


    /**
     * This class is responsible for actually executing the vectorized function at runtime.
     * <p>
     * It will perform bulk transfer-to-python operations on the data,
     * then execute the vectorized function on the results,
     * and return an index of all matching items.
     */
    static class PythonVectorKernel implements FilterKernel<PythonVectorKernel.Context> {

        private final NumbaCompileTools.NumbaEvaluator numbaEvaluator;
        private final JpyModule jpy;

        PythonVectorKernel(
                PyObject vectorized,
                LinkedHashMap<String, String> usedColumns, Class[] usedColumnsTypes) {
            numbaEvaluator = new NumbaCompileTools.NumbaEvaluator(vectorized,usedColumnsTypes,usedColumns.values().toArray(new String[0]));
            jpy = JpyModule.create();
        }

        class Context extends FilterKernel.Context {

            private final NumbaCompileTools.NumbaEvaluator.Context numbaEvaluatorContext;

            Context(int maxChunkSize) {
                super(maxChunkSize);
                numbaEvaluatorContext = numbaEvaluator.getContext();
            }

            @Override
            public void close() {
                resultChunk.close();
            }
        }

        @Override
        public Context getContext(int maxChunkSize) {
            return new Context(maxChunkSize);
        }

        @Override
        public LongChunk<Attributes.OrderedKeyIndices> filter(Context context, LongChunk<Attributes.OrderedKeyIndices> indices, Chunk... inputChunks) {
            final int size = indices.size();

            // TODO: IDS-6241
            // this results in: Value: cannot convert a Python 'numpy.bool_' to a Java 'java.lang.Object'
            /*
            final Boolean[] matches;
            try (final PyObject result = numbaEvaluator.numbaEvaluate(context.numbaEvaluatorContext,inputChunks,size)) {
                matches = result.getObjectArrayValue(Boolean.class);
            }
            */

            // Regardless, this is *hacky*, even without resorting to jpy.to_boolean_array.
            // numbaEvaluator.numbaEvaluate *should* be responsible for returning proper types
            final boolean[] matches;
            try (final PyObject result = numbaEvaluator.numbaEvaluate(context.numbaEvaluatorContext,inputChunks,size)) {
                matches = jpy.to_boolean_array(result);
            }

            context.resultChunk.setSize(0);
            for (int i = 0; i < matches.length; i++) {
                if (matches[i]) {
                    context.resultChunk.add(indices.get(i));
                }
            }
            return context.resultChunk;
        }

    }
}
