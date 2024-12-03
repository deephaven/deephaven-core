//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.Pair;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.impl.select.codegen.FormulaAnalyzer;
import io.deephaven.engine.table.impl.select.python.ArgumentsChunked;
import io.deephaven.engine.table.impl.select.python.DeephavenCompatibleFunction;
import io.deephaven.engine.util.PyCallableWrapperJpyImpl;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.TimeLiteralReplacedExpression;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.select.DhFormulaColumn.COLUMN_SUFFIX;

public abstract class AbstractConditionFilter extends WhereFilterImpl {
    private static final Logger log = LoggerFactory.getLogger(AbstractConditionFilter.class);
    final Map<String, String> outerToInnerNames;
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
    private Pair<String, Map<Long, List<MatchPair>>> formulaShiftColPair;

    protected AbstractConditionFilter(@NotNull String formula, boolean unboxArguments) {
        this.formula = formula;
        this.unboxArguments = unboxArguments;
        this.outerToInnerNames = Collections.emptyMap();
    }

    protected AbstractConditionFilter(@NotNull String formula, Map<String, String> renames, boolean unboxArguments) {
        this.formula = formula;
        this.outerToInnerNames = renames;
        this.unboxArguments = unboxArguments;
    }

    @Override
    public List<String> getColumns() {
        return usedColumns.stream()
                .map(name -> outerToInnerNames.getOrDefault(name, name))
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getColumnArrays() {
        return usedColumnArrays.stream()
                .map(name -> outerToInnerNames.getOrDefault(name, name))
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public void init(@NotNull TableDefinition tableDefinition) {
        init(tableDefinition, QueryCompilerRequestProcessor.immediate());
    }

    @Override
    public synchronized void init(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
        if (initialized) {
            return;
        }

        try {
            final QueryLanguageParser.Result result = FormulaAnalyzer.parseFormula(
                    formula, tableDefinition.getColumnNameMap(), outerToInnerNames,
                    compilationProcessor.getFormulaImports(), unboxArguments);

            formulaShiftColPair = result.getFormulaShiftColPair();
            if (formulaShiftColPair != null) {
                log.debug("Formula (after shift conversion) : " + formulaShiftColPair.getFirst());

                // apply renames to shift column pairs immediately
                if (!outerToInnerNames.isEmpty()) {
                    final Map<Long, List<MatchPair>> shifts = formulaShiftColPair.getSecond();
                    for (Map.Entry<Long, List<MatchPair>> entry : shifts.entrySet()) {
                        List<MatchPair> pairs = entry.getValue();
                        ArrayList<MatchPair> resultPairs = new ArrayList<>(pairs.size());
                        for (MatchPair pair : pairs) {
                            if (outerToInnerNames.containsKey(pair.rightColumn())) {
                                final String newRightColumn = outerToInnerNames.get(pair.rightColumn());
                                resultPairs.add(new MatchPair(pair.leftColumn(), newRightColumn));
                            } else {
                                resultPairs.add(pair);
                            }
                        }
                        entry.setValue(resultPairs);
                    }
                }
            }

            log.debug("Expression (after language conversion) : " + result.getConvertedExpression());

            usedColumns = new ArrayList<>();
            usedColumnArrays = new ArrayList<>();

            final List<QueryScopeParam<?>> paramsList = new ArrayList<>();
            for (String variable : result.getVariablesUsed()) {
                final String columnToFind = outerToInnerNames.getOrDefault(variable, variable);
                final String arrayColumnToFind;
                final String arrayColumnOuterName;
                if (variable.endsWith(COLUMN_SUFFIX)) {
                    arrayColumnOuterName = variable.substring(0, variable.length() - COLUMN_SUFFIX.length());
                    arrayColumnToFind = outerToInnerNames.getOrDefault(arrayColumnOuterName, arrayColumnOuterName);
                } else {
                    arrayColumnToFind = null;
                    arrayColumnOuterName = null;
                }

                if (variable.equals("i")) {
                    usesI = true;
                } else if (variable.equals("ii")) {
                    usesII = true;
                } else if (variable.equals("k")) {
                    usesK = true;
                } else if (tableDefinition.getColumn(columnToFind) != null) {
                    usedColumns.add(variable);
                } else if (arrayColumnToFind != null && tableDefinition.getColumn(arrayColumnToFind) != null) {
                    usedColumnArrays.add(arrayColumnOuterName);
                } else if (result.getPossibleParams().containsKey(variable)) {
                    paramsList.add(new QueryScopeParam<>(variable, result.getPossibleParams().get(variable)));
                }
            }
            params = paramsList.toArray(QueryScopeParam[]::new);

            checkAndInitializeVectorization(result, paramsList);
            if (!initialized) {
                final Class<?> resultType = result.getType();
                checkReturnType(result, resultType);

                generateFilterCode(tableDefinition, result.getTimeConversionResult(), result, compilationProcessor);
                initialized = true;
            }
        } catch (Exception e) {
            throw new FormulaCompilationException("Formula compilation error for: " + formula, e);
        }
    }

    @Override
    public void validateSafeForRefresh(BaseTable<?> sourceTable) {
        if (sourceTable.hasAttribute(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE)) {
            // allow any tests to use i, ii, and k without throwing an exception; we're probably using it safely
            return;
        }
        if (sourceTable.isRefreshing() && !AbstractFormulaColumn.ALLOW_UNSAFE_REFRESHING_FORMULAS) {
            // note that constant offset array accesss does not use i/ii or end up in usedColumnArrays
            boolean isUnsafe = (usesI || usesII) && !sourceTable.isAppendOnly() && !sourceTable.isBlink();
            isUnsafe |= usesK && !sourceTable.isAddOnly() && !sourceTable.isBlink();
            isUnsafe |= !usedColumnArrays.isEmpty() && !sourceTable.isBlink();
            if (isUnsafe) {
                throw new IllegalArgumentException("Formula '" + formula + "' uses i, ii, k, or column array " +
                        "variables, and is not safe to refresh. Note that some usages, such as on an append-only " +
                        "table are safe. To allow unsafe refreshing formulas, set the system property " +
                        "io.deephaven.engine.table.impl.select.AbstractFormulaColumn.allowUnsafeRefreshingFormulas.");
            }
        }
    }

    private void checkAndInitializeVectorization(QueryLanguageParser.Result result,
            List<QueryScopeParam<?>> paramsList) {

        // noinspection SuspiciousToArrayCall
        final PyCallableWrapperJpyImpl[] cws = paramsList.stream()
                .filter(p -> p.getValue() instanceof PyCallableWrapperJpyImpl)
                .map(QueryScopeParam::getValue)
                .toArray(PyCallableWrapperJpyImpl[]::new);
        if (cws.length != 1) {
            return;
        }
        final PyCallableWrapperJpyImpl pyCallableWrapper = cws[0];

        if (pyCallableWrapper.isVectorizable()) {
            checkReturnType(result, pyCallableWrapper.getSignature().getReturnType());

            for (String variable : result.getVariablesUsed()) {
                switch (variable) {
                    case "i":
                        usesI = true;
                        usedColumns.add("i");
                        break;
                    case "ii":
                        usesII = true;
                        usedColumns.add("ii");
                        break;
                    case "k":
                        usesK = true;
                        usedColumns.add("k");
                        break;
                }
            }
            ArgumentsChunked argumentsChunked = pyCallableWrapper.buildArgumentsChunked(usedColumns);
            PyObject vectorized = pyCallableWrapper.vectorizedCallable();
            DeephavenCompatibleFunction dcf = DeephavenCompatibleFunction.create(vectorized,
                    pyCallableWrapper.getSignature().getReturnType(), usedColumns.toArray(new String[0]),
                    argumentsChunked, true);
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

    protected abstract void generateFilterCode(
            @NotNull TableDefinition tableDefinition,
            @NotNull TimeLiteralReplacedExpression timeConversionResult,
            @NotNull QueryLanguageParser.Result result,
            @NotNull QueryCompilerRequestProcessor compilationProcessor)
            throws MalformedURLException, ClassNotFoundException;

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
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
            copy.formulaShiftColPair = formulaShiftColPair;
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

    /**
     * @return true if the formula expression of the filter has Array Access that conforms to "i +/- &lt;constant&gt;"
     *         or "ii +/- &lt;constant&gt;".
     */
    public boolean hasConstantArrayAccess() {
        return getFormulaShiftColPair() != null;
    }

    @Override
    public boolean hasVirtualRowVariables() {
        return usesI || usesII || usesK;
    }

    /**
     * @return a Pair object, consisting of formula string and shift to column MatchPairs, if the filter formula or
     *         expression has Array Access that conforms to "i +/- &lt;constant&gt;" or "ii +/- &lt;constant&gt;". If
     *         there is a parsing error for the expression null is returned.
     */
    public Pair<String, Map<Long, List<MatchPair>>> getFormulaShiftColPair() {
        return formulaShiftColPair;
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
