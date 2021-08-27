package io.deephaven.db.v2.select;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.db.v2.select.python.DeephavenCompatibleFunction;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.lang.DBLanguageParser;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.select.Param;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.*;

import static io.deephaven.db.util.PythonScopeJpyImpl.*;
import static io.deephaven.db.v2.select.DhFormulaColumn.COLUMN_SUFFIX;

public abstract class AbstractConditionFilter extends SelectFilterImpl {
    private static final Logger log = ProcessEnvironment.getDefaultLog(AbstractConditionFilter.class);
    final Map<String, String> outerToInnerNames;
    final Map<String, String> innerToOuterNames;
    @NotNull
    protected final String formula;
    List<String> usedColumns;
    protected Param<?>[] params;
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
            final Map<String, Param<?>> possibleParams = new HashMap<>();
            final QueryScope queryScope = QueryScope.getScope();
            for (Param<?> param : queryScope.getParams(queryScope.getParamNames())) {
                possibleParams.put(param.getName(), param);
                possibleVariables.put(param.getName(), param.getDeclaredType());
            }

            Class<?> compType;
            for (ColumnDefinition<?> column : tableDefinition.getColumns()) {
                final Class<?> dbArrayType = DhFormulaColumn.getDbArrayType(column.getDataType());
                final String columnName = innerToOuterNames.getOrDefault(column.getName(), column.getName());

                possibleVariables.put(columnName, column.getDataType());
                possibleVariables.put(columnName + COLUMN_SUFFIX, dbArrayType);

                compType = column.getComponentType();
                if (compType != null && !compType.isPrimitive()) {
                    possibleVariableParameterizedTypes.put(columnName, new Class[] {compType});
                }
                if (dbArrayType == DbArray.class) {
                    possibleVariableParameterizedTypes.put(columnName + COLUMN_SUFFIX,
                            new Class[] {column.getDataType()});
                }
            }

            log.debug("Expression (before) : " + formula);

            final DBTimeUtils.Result timeConversionResult = DBTimeUtils.convertExpression(formula);

            log.debug("Expression (after time conversion) : " + timeConversionResult.getConvertedFormula());

            possibleVariables.putAll(timeConversionResult.getNewVariables());

            final DBLanguageParser.Result result = new DBLanguageParser(timeConversionResult.getConvertedFormula(),
                    QueryLibrary.getPackageImports(), QueryLibrary.getClassImports(), QueryLibrary.getStaticImports(),
                    possibleVariables, possibleVariableParameterizedTypes, unboxArguments).getResult();

            log.debug("Expression (after language conversion) : " + result.getConvertedExpression());

            usedColumns = new ArrayList<>();
            usedColumnArrays = new ArrayList<>();

            final List<Param<?>> paramsList = new ArrayList<>();
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
            params = paramsList.toArray(Param.ZERO_LENGTH_PARAM_ARRAY);

            // check if this is a filter that uses a numba vectorized function
            Optional<Param<?>> paramOptional =
                    Arrays.stream(params).filter(p -> p.getValue() instanceof NumbaCallableWrapper).findFirst();
            if (paramOptional.isPresent()) {
                /*
                 * numba vectorized function must be used alone as an entire expression, and that should have been
                 * checked in the DBLanguageParser already, this is a sanity check
                 */
                if (params.length != 1) {
                    throw new UncheckedDeephavenException(
                            "internal error - misuse of numba vectorized functions wasn't detected.");
                }

                NumbaCallableWrapper numbaCallableWrapper = (NumbaCallableWrapper) paramOptional.get().getValue();
                DeephavenCompatibleFunction dcf = DeephavenCompatibleFunction.create(numbaCallableWrapper.getPyObject(),
                        numbaCallableWrapper.getReturnType(), usedColumns.toArray(new String[0]),
                        true);
                checkReturnType(result, dcf.getReturnedType());
                setFilter(new ConditionFilter.ChunkFilter(
                        dcf.toFilterKernel(),
                        dcf.getColumnNames().toArray(new String[0]),
                        ConditionFilter.CHUNK_SIZE));
                initialized = true;
                return;
            }

            final Class<?> resultType = result.getType();
            checkReturnType(result, resultType);

            generateFilterCode(tableDefinition, timeConversionResult, result);
            initialized = true;
        } catch (Exception e) {
            throw new FormulaCompilationException("Formula compilation error for: " + formula, e);
        }
    }

    private void checkReturnType(DBLanguageParser.Result result, Class<?> resultType) {
        if (!Boolean.class.equals(resultType) && !boolean.class.equals(resultType)) {
            throw new RuntimeException("Invalid condition filter expression type: boolean required.\n" +
                    "Formula              : " + truncateLongFormula(formula) + '\n' +
                    "Converted Expression : " + truncateLongFormula(result.getConvertedExpression()) + '\n' +
                    "Expression Type      : " + resultType.getName());
        }
    }

    protected abstract void generateFilterCode(TableDefinition tableDefinition, DBTimeUtils.Result timeConversionResult,
            DBLanguageParser.Result result) throws MalformedURLException, ClassNotFoundException;

    @Override
    public Index filter(Index selection, Index fullSet, Table table, boolean usePrev) {
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

    protected abstract Filter getFilter(Table table, Index fullSet)
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
        Index filter(
                Index selection,
                Index fullSet,
                Table table,
                boolean usePrev,
                String formula,
                Param<?>... params);
    }

    static String truncateLongFormula(String formula) {
        if (formula.length() > 128) {
            formula = formula.substring(0, 128) + " [truncated]";
        }
        return formula;
    }
}
