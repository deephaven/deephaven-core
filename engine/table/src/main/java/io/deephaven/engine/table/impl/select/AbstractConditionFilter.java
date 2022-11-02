/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.impl.select.python.ArgumentsChunked;
import io.deephaven.engine.table.impl.select.python.DeephavenCompatibleFunction;
import io.deephaven.engine.util.PyCallableWrapper;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.deephaven.engine.table.impl.select.DhFormulaColumn.COLUMN_SUFFIX;

public abstract class AbstractConditionFilter extends WhereFilterImpl {
    private static final Logger log = LoggerFactory.getLogger(AbstractConditionFilter.class);
    final Map<String, String> outerToInnerNames;
    final Map<String, String> innerToOuterNames;
    @NotNull
    protected final String formula;
    List<String> usedColumns;
    protected QueryScopeParam<?>[] params;
    List<String> usedColumnArrays;
    protected boolean initialized = false;
    boolean usesI;
    boolean usesII;
    boolean usesK;
    private final boolean unboxArguments;


    protected AbstractConditionFilter(@NotNull String formula, boolean unboxArguments) {
        this.formula = formula;
        this.unboxArguments = unboxArguments;
        this.outerToInnerNames = Collections.emptyMap();
        this.innerToOuterNames = Collections.emptyMap();
    }

    protected AbstractConditionFilter(@NotNull String formula, Map<String, String> renames, boolean unboxArguments) {
        this.formula = formula;
        this.outerToInnerNames = renames;
        this.unboxArguments = unboxArguments;
        this.innerToOuterNames = new HashMap<>();
        for (Map.Entry<String, String> outerInnerEntry : outerToInnerNames.entrySet()) {
            innerToOuterNames.put(outerInnerEntry.getValue(), outerInnerEntry.getKey());
        }
    }

    @Override
    public List<String> getColumns() {
        return usedColumns;
    }

    @Override
    public List<String> getColumnArrays() {
        return usedColumnArrays;
    }

    @Override
    public synchronized void init(TableDefinition tableDefinition) {
        if (initialized) {
            return;
        }

        final Map<String, Class<?>> possibleVariables = new HashMap<>();
        possibleVariables.put("i", int.class);
        possibleVariables.put("ii", long.class);
        possibleVariables.put("k", long.class);

        final Map<String, Class<?>[]> possibleVariableParameterizedTypes = new HashMap<>();

        try {
            final Map<String, QueryScopeParam<?>> possibleParams = new HashMap<>();
            final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
            for (QueryScopeParam<?> param : queryScope.getParams(queryScope.getParamNames())) {
                possibleParams.put(param.getName(), param);
                possibleVariables.put(param.getName(), QueryScopeParamTypeUtil.getDeclaredClass(param.getValue()));
                Type declaredType = QueryScopeParamTypeUtil.getDeclaredType(param.getValue());
                if (declaredType instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) declaredType;
                    Class<?>[] paramTypes = Arrays.stream(pt.getActualTypeArguments())
                            .map(QueryScopeParamTypeUtil::classFromType)
                            .toArray(Class<?>[]::new);
                    possibleVariableParameterizedTypes.put(param.getName(), paramTypes);
                }
            }

            Class<?> compType;
            for (ColumnDefinition<?> column : tableDefinition.getColumns()) {
                final Class<?> vectorType = DhFormulaColumn.getVectorType(column.getDataType());
                final String columnName = innerToOuterNames.getOrDefault(column.getName(), column.getName());

                possibleVariables.put(columnName, column.getDataType());
                possibleVariables.put(columnName + COLUMN_SUFFIX, vectorType);

                compType = column.getComponentType();
                if (compType != null && !compType.isPrimitive()) {
                    possibleVariableParameterizedTypes.put(columnName, new Class[] {compType});
                }
                if (vectorType == ObjectVector.class) {
                    possibleVariableParameterizedTypes.put(columnName + COLUMN_SUFFIX,
                            new Class[] {column.getDataType()});
                }
            }

            log.debug("Expression (before) : " + formula);

            final DateTimeUtils.Result timeConversionResult = DateTimeUtils.convertExpression(formula);

            log.debug("Expression (after time conversion) : " + timeConversionResult.getConvertedFormula());

            possibleVariables.putAll(timeConversionResult.getNewVariables());

            final QueryLanguageParser.Result result =
                    new QueryLanguageParser(timeConversionResult.getConvertedFormula(),
                            ExecutionContext.getContext().getQueryLibrary().getPackageImports(),
                            ExecutionContext.getContext().getQueryLibrary().getClassImports(),
                            ExecutionContext.getContext().getQueryLibrary().getStaticImports(),
                            possibleVariables, possibleVariableParameterizedTypes, unboxArguments).getResult();

            log.debug("Expression (after language conversion) : " + result.getConvertedExpression());

            usedColumns = new ArrayList<>();
            usedColumnArrays = new ArrayList<>();

            final List<QueryScopeParam<?>> paramsList = new ArrayList<>();
            for (String variable : result.getVariablesUsed()) {
                final String columnToFind = outerToInnerNames.getOrDefault(variable, variable);
                final String arrayColumnToFind;
                if (variable.endsWith(COLUMN_SUFFIX)) {
                    final String originalName = variable.substring(0, variable.length() - COLUMN_SUFFIX.length());
                    arrayColumnToFind = outerToInnerNames.getOrDefault(originalName, originalName);
                } else {
                    arrayColumnToFind = null;
                }

                if (variable.equals("i")) {
                    usesI = true;
                } else if (variable.equals("ii")) {
                    usesII = true;
                } else if (variable.equals("k")) {
                    usesK = true;
                } else if (tableDefinition.getColumn(columnToFind) != null) {
                    usedColumns.add(columnToFind);
                } else if (arrayColumnToFind != null && tableDefinition.getColumn(arrayColumnToFind) != null) {
                    usedColumnArrays.add(arrayColumnToFind);
                } else if (possibleParams.containsKey(variable)) {
                    paramsList.add(possibleParams.get(variable));
                }
            }
            params = paramsList.toArray(QueryScopeParam.ZERO_LENGTH_PARAM_ARRAY);

            checkAndInitializeVectorization(result, paramsList);
            if (!initialized) {
                final Class<?> resultType = result.getType();
                checkReturnType(result, resultType);

                generateFilterCode(tableDefinition, timeConversionResult, result);
                initialized = true;
            }
        } catch (Exception e) {
            throw new FormulaCompilationException("Formula compilation error for: " + formula, e);
        }
    }

    private void checkAndInitializeVectorization(QueryLanguageParser.Result result,
            List<QueryScopeParam<?>> paramsList) {

        PyCallableWrapper[] cws = paramsList.stream().filter(p -> p.getValue() instanceof PyCallableWrapper)
                .map(p -> p.getValue()).toArray(PyCallableWrapper[]::new);
        if (cws.length != 1) {
            return;
        }
        PyCallableWrapper pyCallableWrapper = cws[0];

        if (pyCallableWrapper.isVectorizable()) {
            for (String variable : result.getVariablesUsed()) {
                if (variable.equals("i")) {
                    usesI = true;
                    usedColumns.add("i");
                } else if (variable.equals("ii")) {
                    usesII = true;
                    usedColumns.add("ii");
                } else if (variable.equals("k")) {
                    usesK = true;
                    usedColumns.add("k");
                }
            }

            ArgumentsChunked argumentsChunked = pyCallableWrapper.buildArgumentsChunked(usedColumns);
            PyObject vectorized = pyCallableWrapper.vectorizedCallable();
            DeephavenCompatibleFunction dcf = DeephavenCompatibleFunction.create(vectorized,
                    pyCallableWrapper.getReturnType(), usedColumns.toArray(new String[0]), argumentsChunked, true);
            checkReturnType(result, dcf.getReturnedType());
            setFilter(new ConditionFilter.ChunkFilter(
                    dcf.toFilterKernel(),
                    dcf.getColumnNames().toArray(new String[0]),
                    ConditionFilter.CHUNK_SIZE));
            initialized = true;
        }
    }

    private void checkReturnType(QueryLanguageParser.Result result, Class<?> resultType) {
        if (!Boolean.class.equals(resultType) && !boolean.class.equals(resultType)) {
            throw new RuntimeException("Invalid condition filter expression type: boolean required.\n" +
                    "Formula              : " + truncateLongFormula(formula) + '\n' +
                    "Converted Expression : " + truncateLongFormula(result.getConvertedExpression()) + '\n' +
                    "Expression Type      : " + resultType.getName());
        }
    }

    protected abstract void generateFilterCode(TableDefinition tableDefinition,
            DateTimeUtils.Result timeConversionResult,
            QueryLanguageParser.Result result) throws MalformedURLException, ClassNotFoundException;

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        if (usePrev && params.length > 0) {
            throw new PreviousFilteringNotSupported("Previous filter with parameters not supported.");
        }

        final Filter filter;
        try {
            filter = getFilter(table, fullSet);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate filter class", e);
        }
        return filter.filter(selection, fullSet, table, usePrev, formula, params);
    }

    protected abstract Filter getFilter(Table table, RowSet fullSet)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException;

    /**
     * When numba vectorized functions are used to evaluate query filters, we need to create a special ChunkFilter that
     * can handle packing and unpacking arrays required/returned by the vectorized function, essentially bypassing the
     * regular code generation process which isn't able to support such use cases without needing some major rework.
     *
     * @param filter the filter to set
     */
    protected abstract void setFilter(Filter filter);

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}

    @Override
    public abstract AbstractConditionFilter copy();

    protected void onCopy(final AbstractConditionFilter copy) {
        if (initialized) {
            copy.initialized = true;
            copy.usedColumns = usedColumns;
            copy.usedColumnArrays = usedColumnArrays;
            copy.usesI = usesI;
            copy.usesII = usesII;
            copy.usesK = usesK;
            copy.params = params;
        }
    }

    @Override
    public String toString() {
        return formula;
    }

    @Override
    public boolean isSimpleFilter() {
        return false;
    }

    public abstract AbstractConditionFilter renameFilter(Map<String, String> renames);

    public interface Filter {
        /**
         * See {@link WhereFilter#filter(RowSet, RowSet, Table, boolean)} for basic documentation of {@code selection},
         * {@code fullSet}, {@code table}, and {@code usePrev}.
         */
        WritableRowSet filter(
                RowSet selection,
                RowSet fullSet,
                Table table,
                boolean usePrev,
                String formula,
                QueryScopeParam<?>... params);
    }

    static String truncateLongFormula(String formula) {
        if (formula.length() > 128) {
            formula = formula.substring(0, 128) + " [truncated]";
        }
        return formula;
    }
}
