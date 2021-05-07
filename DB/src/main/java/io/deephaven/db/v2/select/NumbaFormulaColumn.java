package io.deephaven.db.v2.select;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.select.Param;
import io.deephaven.db.v2.select.formula.FormulaKernel;
import io.deephaven.db.v2.select.formula.FormulaKernelFactory;
import io.deephaven.db.v2.select.formula.FormulaSourceDescriptor;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.util.type.TypeUtils;
import org.jpy.PyDictWrapper;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

public class NumbaFormulaColumn extends AbstractFormulaColumn {

    private boolean initialized;
    private LinkedHashMap<String, String> usedCols;
    private FormulaSourceDescriptor formulaSourceDescriptor;
    private PyObject theFunction;
    private Object returnValue;
    private Map<String, ColumnDefinition> expandedColumnNameMap;

    /**
     * Create a formula column for the given formula string.
     * <p>
     * The internal formula object is generated on-demand by calling out to the Java compiler.
     *
     * @param columnName    the result column name
     * @param formulaString the formula string to be parsed by the DBLanguageParser
     */
    NumbaFormulaColumn(String columnName, String formulaString) {
        super(columnName, formulaString, true);
    }

    @Override
    protected FormulaSourceDescriptor getSourceDescriptor() {
        return formulaSourceDescriptor;
    }

    @Override
    protected FormulaKernelFactory getFormulaKernelFactory() {

        return new PythonKernelFactory(theFunction, usedCols.values().toArray(ZERO_LENGTH_STRING_ARRAY),
                usedCols.keySet().stream().map(name -> expandedColumnNameMap.get(name).getDataType()).toArray(Class[]::new), returnedType, returnValue);
    }

    @Override
    public List<String> initDef(Map<String, ColumnDefinition> columnNameMap) {
        if (!initialized) {
            try {
                initialized = true;
                expandedColumnNameMap = NumbaCompileTools.getExpandedColumnNames(columnNameMap);
                PyDictWrapper vectorized = NumbaCompileTools.compileFormula(formulaString, expandedColumnNameMap);
                usedCols = NumbaCompileTools.findUsedColumns(vectorized, expandedColumnNameMap);

                if (vectorized.containsKey("fun")) {
                    theFunction = vectorized.get("fun");
                } else {
                    theFunction = null;
                    returnValue = vectorized.get("result").getObjectValue();
                }

                Map<String, ColumnDefinition> finalColumnNameMap = expandedColumnNameMap;
                Class[] usedColumnsTypes = usedCols.keySet().stream().map(name -> finalColumnNameMap.get(name).getDataType()).toArray(Class[]::new);

                returnedType = theFunction != null ? NumbaCompileTools.toJavaType(vectorized.get("return_type").toString()) : TypeUtils.getUnboxedType(returnValue.getClass());
                if (theFunction == null && returnedType == null) {
                    returnedType = returnValue.getClass();
                }

                formulaSourceDescriptor = new FormulaSourceDescriptor(returnedType, usedCols.keySet().toArray(ZERO_LENGTH_STRING_ARRAY),
                        ZERO_LENGTH_STRING_ARRAY, ZERO_LENGTH_STRING_ARRAY);


                applyUsedVariables(expandedColumnNameMap, usedCols.keySet());

            } catch (Exception e) {
                throw new FormulaCompilationException("Formula compilation error for: " + formulaString, e);
            }
        }
        return usedColumns;
    }

    @Override
    public SelectColumn copy() {
        return new NumbaFormulaColumn(columnName, formulaString);
    }

    private static class PythonKernelFactory implements FormulaKernelFactory {

        private final PyObject vectorized;
        private final String[] columnsNpTypeName;
        private final Class[] usedColumnsTypes;
        private final Class returnType;
        private final Object returnValue;

        private PythonKernelFactory(PyObject vectorized, String columnsNpTypeName[], Class[] usedColumnsTypes, Class returnType, Object returnValue) {
            this.vectorized = vectorized;
            this.columnsNpTypeName = columnsNpTypeName;
            this.usedColumnsTypes = usedColumnsTypes;
            this.returnType = returnType;
            this.returnValue = returnValue;
        }

        @Override
        public FormulaKernel createInstance(DbArrayBase[] arrays, Param[] params) {
            return new PythonKernel(vectorized, columnsNpTypeName, usedColumnsTypes, returnType, returnValue);
        }

    }


    private static class PythonKernel implements FormulaKernel {
        private final PyObject vectorized;
        private final Class[] usedColumnsTypes;
        private final Class returnType;
        private final PyModule np;
        private final PyModule jpy;
        private Object result;
        private final NumbaCompileTools.NumbaEvaluator evaluator;

        public PythonKernel(
                PyObject vectorized,
                String[] columnsNpTypeName, Class[] usedColumnsTypes, Class returnType, Object constResult) {
            this.vectorized = vectorized;
            this.usedColumnsTypes = usedColumnsTypes;
            this.returnType = returnType;
            this.result = constResult;
            this.np = PyModule.importModule("numpy");
            this.jpy = PyModule.importModule("jpy");
            evaluator = new NumbaCompileTools.NumbaEvaluator(vectorized, usedColumnsTypes, columnsNpTypeName);
        }

        @Override
        public Formula.FillContext makeFillContext(int maxChunkSize) {
            return new Context(maxChunkSize, usedColumnsTypes, returnType);
        }

        @Override
        public void applyFormulaChunk(Formula.FillContext contextArg, WritableChunk<? super Attributes.Values> destination, Chunk<? extends Attributes.Values>[] inputChunks) {
            Context context = (Context) contextArg;
            final int size = destination.size();
            if (vectorized == null) {
                destination.fillWithBoxedValue(0, size, result);
                return;
            }

            try (final PyObject result = evaluator.numbaEvaluate(context.evaluatorContext, inputChunks, size)) {
                if (returnType == Boolean.class) {
                    final Boolean[] objVal = result.getObjectArrayValue(Boolean.class);
                    WritableObjectChunk dest = destination.asWritableObjectChunk();
                    for (int i = 0; i < destination.size(); i++) {
                        dest.set(i, objVal[i]);
                    }
                } else {
                    // todo: this won't work, I think we need to make sure it's a *primitive* array.
                    //final Object[] objVal = result.getObjectArrayValue(returnType);

                    final Object objVal;
                    try (final PyObject pyObj = jpy.call("array", returnType.getName(), result)) {
                        objVal = pyObj.getObjectValue();
                    }
                    destination.copyFromArray(objVal, 0, 0, destination.size());
                }
            }
        }

        class Context implements Formula.FillContext {

            public final int chunkSize;
            private final WritableChunk<Attributes.Any> resultChunk;
            private final NumbaCompileTools.NumbaEvaluator.Context evaluatorContext;


            public Context(int maxChunkSize, Class[] usedColumnsTypes, Class returnType) {
                resultChunk = ChunkType.fromElementType(returnType).makeWritableChunk(maxChunkSize);
                this.chunkSize = maxChunkSize;
                this.evaluatorContext = evaluator.getContext();
            }

            @Override
            public void close() {
                resultChunk.close();
            }
        }

    }
}
