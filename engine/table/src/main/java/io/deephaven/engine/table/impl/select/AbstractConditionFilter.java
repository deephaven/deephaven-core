//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.Pair;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnDefinition;
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
import org.jetbrains.annotations.Nullable;
import org.jpy.PyObject;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.select.DhFormulaColumn.COLUMN_SUFFIX;

public abstract class AbstractConditionFilter extends WhereFilterImpl implements RenameableFilter {
    private static final Logger log = LoggerFactory.getLogger(AbstractConditionFilter.class);
    final Map<String, String> outerToInnerNames;
    /**
     * The formula input this filter reads from columns, recorded when the filter is renamed; null if the filter was
     * never renamed. A renamed filter is executed before a view, on the view's source table, which may have columns
     * that the view hides. Restricting column reads to this set keeps any other input, such as a query-scope variable,
     * from reading a hidden column of the same name.
     */
    @Nullable
    final Set<String> formulaColumns;
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
    private Pair<String, Set<ShiftedColumnDefinition>> formulaShiftedColumnDefinitions;

    protected AbstractConditionFilter(@NotNull String formula, boolean unboxArguments) {
        this.formula = formula;
        this.unboxArguments = unboxArguments;
        this.outerToInnerNames = Collections.emptyMap();
        this.formulaColumns = null;
    }

    protected AbstractConditionFilter(@NotNull String formula, Map<String, String> renames,
            @Nullable Set<String> formulaColumns, boolean unboxArguments) {
        this.formula = formula;
        this.outerToInnerNames = renames;
        this.formulaColumns = formulaColumns;
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
            final QueryLanguageParser.Result result;
            if (formulaColumns == null) {
                result = FormulaAnalyzer.parseFormula(
                        formula, tableDefinition.getColumnNameMap(), outerToInnerNames,
                        compilationProcessor.getFormulaImports(), unboxArguments);
            } else {
                // offer the parser a column for each formula input in formulaColumns, as the formula writes the input,
                // and no other column, so the other columns of the table being filtered cannot shadow a query-scope
                // variable
                final Map<String, ColumnDefinition<?>> availableColumns = new LinkedHashMap<>();
                for (final String formulaColumn : formulaColumns) {
                    final ColumnDefinition<?> column = tableDefinition.getColumn(
                            outerToInnerNames.getOrDefault(formulaColumn, formulaColumn));
                    if (column != null) {
                        availableColumns.put(formulaColumn, column.withName(formulaColumn));
                    }
                }
                result = FormulaAnalyzer.parseFormula(formula, availableColumns, Collections.emptyMap(),
                        compilationProcessor.getFormulaImports(), unboxArguments);
            }

            formulaShiftedColumnDefinitions = result.getShiftedColumnDefinitions();
            if (formulaShiftedColumnDefinitions != null) {
                log.debug().append("Formula (after shift conversion) : ")
                        .append(formulaShiftedColumnDefinitions.getSecond().stream()
                                .map(ShiftedColumnDefinition::toString)
                                .collect(Collectors.joining(", ")))
                        .endl();

                // apply renames to shift column pairs immediately
                if (!outerToInnerNames.isEmpty()) {
                    final Map<String, String> shiftedColumnRenames = new HashMap<>();
                    final Set<ShiftedColumnDefinition> resultSet = new LinkedHashSet<>();
                    for (final ShiftedColumnDefinition shift : formulaShiftedColumnDefinitions.getSecond()) {
                        if (outerToInnerNames.containsKey(shift.getColumnName())) {
                            final String innerName = outerToInnerNames.get(shift.getColumnName());
                            final ShiftedColumnDefinition newDefinition =
                                    new ShiftedColumnDefinition(innerName, shift.getShiftAmount());
                            resultSet.add(newDefinition);
                            shiftedColumnRenames.put(shift.getResultColumnName(), newDefinition.getResultColumnName());
                        } else {
                            resultSet.add(shift);
                        }
                    }
                    String newFormula = formulaShiftedColumnDefinitions.getFirst();
                    if (!shiftedColumnRenames.isEmpty()) {
                        // replace every shifted column in one pass: a swap maps A to B and B to A, so replacing one
                        // shifted column at a time would replace the first replacement again
                        newFormula = Pattern.compile(shiftedColumnRenames.keySet().stream()
                                .map(Pattern::quote)
                                .collect(Collectors.joining("|")))
                                .matcher(newFormula)
                                .replaceAll(match -> Matcher.quoteReplacement(shiftedColumnRenames.get(match.group())));
                    }
                    formulaShiftedColumnDefinitions = new Pair(newFormula, resultSet);
                }
            }

            log.debug("Expression (after language conversion) : " + result.getConvertedExpression());

            usedColumns = new ArrayList<>();
            usedColumnArrays = new ArrayList<>();

            final List<QueryScopeParam<?>> paramsList = new ArrayList<>();
            for (String variable : result.getVariablesUsed()) {
                final String arrayColumnOuterName = variable.endsWith(COLUMN_SUFFIX)
                        ? variable.substring(0, variable.length() - COLUMN_SUFFIX.length())
                        : null;

                if (variable.equals("i")) {
                    usesI = true;
                } else if (variable.equals("ii")) {
                    usesII = true;
                } else if (variable.equals("k")) {
                    usesK = true;
                } else if (resolvesAsColumn(tableDefinition, variable)) {
                    usedColumns.add(variable);
                } else if (arrayColumnOuterName != null && resolvesAsColumn(tableDefinition, arrayColumnOuterName)) {
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
            // note that constant offset array access does not use i/ii or end up in usedColumnArrays
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
            setPythonFilter(new ConditionFilter.ChunkFilter(
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

    /**
     * Retrieve the current {@link Filter filter} for this condition filter or create a new one initialized to the
     * provided table and row set. With a {@link ConditionFilter.FilterKernel.Context context} from
     * {@link Filter#getContext(int)}, this filter can be used for directly filtering chunked data.
     *
     * @param table the table to filter, or a table with a compatible schema
     * @param fullSet the full set of rows currently in the table, used to populate the virtual row variables such as
     *        {@code i}, {@code ii}, and {@code k}
     *
     * @return the initialized filter
     */
    @NotNull
    public abstract Filter getFilter(Table table, RowSet fullSet)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException;

    /**
     * When numba vectorized functions are used to evaluate query filters, we need to create a special ChunkFilter that
     * can handle packing and unpacking arrays required/returned by the vectorized function, essentially bypassing the
     * regular code generation process which isn't able to support such use cases without needing some major rework.
     *
     * @param filter the filter to set
     */
    protected abstract void setPythonFilter(Filter filter);

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
            copy.formulaShiftedColumnDefinitions = formulaShiftedColumnDefinitions;
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
        return getFormulaShiftedColumnDefinitions() != null;
    }

    /**
     * Returns true if this filters uses row virtual offset columns of {@code i}, {@code ii} or {@code k}.
     * <p>
     * This filter must already be initialized before calling this method.
     */
    public boolean hasVirtualRowVariables() {
        return usesI || usesII || usesK;
    }

    /**
     * @return a mapping from inner-formula-expression to shifted column definitions, consisting of the set of columns
     *         and their shifts that this expression has Array Access that conforms to "i +/- &lt;constant&gt;" or "ii
     *         +/- &lt;constant&gt;". If there is a parsing error for the expression null is returned.
     */
    public Pair<String, Set<ShiftedColumnDefinition>> getFormulaShiftedColumnDefinitions() {
        return formulaShiftedColumnDefinitions;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The renamed filter reads the columns through {@code renames} in addition to any renames this filter already
     * carries. The formula keeps the names the formula was written with, so a filter renamed once per level of nested
     * views composes the renames of every level. The renamed filter offers the formula only the names this filter
     * resolved as columns, as values or as arrays.
     */
    @Override
    public abstract AbstractConditionFilter renameFilter(Map<String, String> renames);

    /**
     * Whether the formula input {@code formulaName} reads a column of {@code tableDefinition}: the input must name a
     * column of the table, through any rename, and a renamed filter must have read the input from a column before the
     * rename.
     */
    private boolean resolvesAsColumn(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final String formulaName) {
        return (formulaColumns == null || formulaColumns.contains(formulaName))
                && tableDefinition.getColumn(outerToInnerNames.getOrDefault(formulaName, formulaName)) != null;
    }

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

        /**
         * Create a new context for this filter, must be closed after use.
         */
        ConditionFilter.FilterKernel.Context getContext(int chunkSize);

        /**
         * Filter a chunk of values and copy the matching row keys to the returned chunk.
         */
        LongChunk<OrderedRowKeys> filter(
                ConditionFilter.FilterKernel.Context context,
                LongChunk<OrderedRowKeys> inputKeys,
                Chunk<? extends Values>[] valueChunks);

        /**
         * Filter a chunk of values, setting parallel values in {@code results} to the output of the filter.
         *
         * @return the number of values are {@code true} in {@code results} after the filter is applied.
         */
        int filter(
                ConditionFilter.FilterKernel.Context context,
                Chunk<? extends Values>[] valueChunks,
                int chunkSize,
                WritableBooleanChunk<Values> results);

        /**
         * Filter a chunk of values, setting parallel values in {@code results} to {@code false} when the filter result
         * is {@code false}. The filter will not be evaluated for values that are currently {@code false} in the results
         * chunk.
         * <p>
         * To use this method effectively, the results chunk should be initialized by a call to
         * {@link #filter(ConditionFilter.FilterKernel.Context, Chunk[], int, WritableBooleanChunk)} or by setting all
         * values to {@code true} before the first call. Successive calls will have the effect of AND'ing this filter
         * results with existing results.
         *
         * @return the number of values are {@code true} in {@code results} after the filter is applied.
         */
        int filterAnd(
                ConditionFilter.FilterKernel.Context context,
                Chunk<? extends Values>[] valueChunks,
                int chunkSize,
                WritableBooleanChunk<Values> results);
    }

    static String truncateLongFormula(String formula) {
        if (formula.length() > 128) {
            formula = formula.substring(0, 128) + " [truncated]";
        }
        return formula;
    }
}
