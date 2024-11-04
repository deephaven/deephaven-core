//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.*;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.engine.table.vectors.*;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.impl.select.formula.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.api.util.NameValidator;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * A SelectColumn that implements a formula computed from the existing columns in the table and a query scope.
 */
public abstract class AbstractFormulaColumn implements FormulaColumn {
    private static final Logger log = LoggerFactory.getLogger(AbstractFormulaColumn.class);

    public static final boolean ALLOW_UNSAFE_REFRESHING_FORMULAS = Configuration.getInstance()
            .getBooleanForClassWithDefault(AbstractFormulaColumn.class, "allowUnsafeRefreshingFormulas", false);


    protected String formulaString;
    protected final String originalFormulaString;
    protected List<String> usedColumns;

    @NotNull
    protected final String columnName;
    protected Future<FormulaFactory> formulaFactoryFuture;
    private Formula formula;
    protected QueryScopeParam<?>[] params;
    protected Map<String, ? extends ColumnSource<?>> columnSources;
    protected Map<String, ColumnDefinition<?>> columnDefinitions;
    private TrackingRowSet rowSet;
    protected Class<?> returnedType;
    public static final String COLUMN_SUFFIX = "_";
    protected List<String> usedColumnArrays;
    protected boolean usesI; // uses the "i" variable which is an integer position for the row
    protected boolean usesII; // uses the "ii" variable which is the long position for the row
    protected boolean usesK; // uses the "k" variable which is the long row key into a column source

    /**
     * Create a formula column for the given formula string.
     * <p>
     * The internal formula object is generated on-demand by calling out to the Java compiler.
     *
     * @param columnName the result column name
     * @param formulaString the formula string to be parsed by the QueryLanguageParser
     */
    protected AbstractFormulaColumn(String columnName, String formulaString) {
        this.formulaString = Require.neqNull(formulaString, "formulaString");
        this.originalFormulaString = formulaString;
        this.columnName = NameValidator.validateColumnName(columnName);
    }

    @Override
    public Class<?> getReturnedType() {
        return returnedType;
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return returnedType.getComponentType();
    }

    @Override
    public List<String> initInputs(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        this.rowSet = rowSet;

        if (usedColumns == null) {
            initDef(extractDefinitions(columnsOfInterest), QueryCompilerRequestProcessor.immediate());
        }
        this.columnSources = filterColumnSources(columnsOfInterest);

        return usedColumns;
    }

    private Map<String, ColumnSource<?>> filterColumnSources(
            final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        if (usedColumns.isEmpty() && usedColumnArrays.isEmpty()) {
            return Map.of();
        }

        final HashMap<String, ColumnSource<?>> sources = new HashMap<>();
        for (String columnName : usedColumns) {
            sources.put(columnName, columnsOfInterest.get(columnName));
        }
        for (String columnName : usedColumnArrays) {
            sources.put(columnName, columnsOfInterest.get(columnName));
        }
        return sources;
    }

    @Override
    public void validateSafeForRefresh(BaseTable<?> sourceTable) {
        if (sourceTable.hasAttribute(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE)) {
            // allow any tests to use i, ii, and k without throwing an exception; we're probably using it safely
            return;
        }
        if (sourceTable.isRefreshing() && !ALLOW_UNSAFE_REFRESHING_FORMULAS) {
            // note that constant offset array accesss does not use i/ii or end up in usedColumnArrays
            boolean isUnsafe = (usesI || usesII) && !sourceTable.isAppendOnly() && !sourceTable.isBlink();
            isUnsafe |= usesK && !sourceTable.isAddOnly() && !sourceTable.isBlink();
            isUnsafe |= !usedColumnArrays.isEmpty() && !sourceTable.isBlink();
            if (isUnsafe) {
                throw new IllegalArgumentException("Formula '" + formulaString + "' uses i, ii, k, or column array " +
                        "variables, and is not safe to refresh. Note that some usages, such as on an append-only " +
                        "table are safe. To allow unsafe refreshing formulas, set the system property " +
                        "io.deephaven.engine.table.impl.select.AbstractFormulaColumn.allowUnsafeRefreshingFormulas.");
            }
        }
    }

    protected void applyUsedVariables(
            @NotNull final Map<String, ColumnDefinition<?>> parentColumnDefinitions,
            @NotNull final Set<String> variablesUsed,
            @NotNull final Map<String, Object> possibleParams) {
        // the column definition map passed in is being mutated by the caller, so we need to make a copy
        columnDefinitions = new HashMap<>();

        final List<QueryScopeParam<?>> paramsList = new ArrayList<>();
        usedColumns = new ArrayList<>();
        usedColumnArrays = new ArrayList<>();
        for (String variable : variablesUsed) {
            ColumnDefinition<?> columnDefinition = parentColumnDefinitions.get(variable);
            if (variable.equals("i")) {
                usesI = true;
            } else if (variable.equals("ii")) {
                usesII = true;
            } else if (variable.equals("k")) {
                usesK = true;
            } else if (columnDefinition != null) {
                columnDefinitions.put(variable, columnDefinition);
                usedColumns.add(variable);
            } else {
                String strippedColumnName =
                        variable.substring(0, Math.max(0, variable.length() - COLUMN_SUFFIX.length()));
                columnDefinition = parentColumnDefinitions.get(strippedColumnName);
                if (variable.endsWith(COLUMN_SUFFIX) && columnDefinition != null) {
                    columnDefinitions.put(strippedColumnName, columnDefinition);
                    usedColumnArrays.add(strippedColumnName);
                } else if (possibleParams.containsKey(variable)) {
                    paramsList.add(new QueryScopeParam<>(variable, possibleParams.get(variable)));
                }
            }
        }

        params = paramsList.toArray(QueryScopeParam[]::new);
    }

    protected void onCopy(final AbstractFormulaColumn copy) {
        copy.formulaFactoryFuture = formulaFactoryFuture;
        copy.columnDefinitions = columnDefinitions;
        copy.params = params;
        copy.usedColumns = usedColumns;
        copy.usedColumnArrays = usedColumnArrays;
        copy.usesI = usesI;
        copy.usesII = usesII;
        copy.usesK = usesK;
    }

    protected void validateColumnDefinition(Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        final Consumer<String> validateColumn = name -> {
            final ColumnDefinition<?> newDef = columnDefinitionMap.get(name);
            final ColumnDefinition<?> origDef = columnDefinitions.get(name);
            if (!origDef.isCompatible(newDef)) {
                throw new IllegalStateException("initDef must be idempotent but column '" + name + "' changed from "
                        + origDef.describeForCompatibility() + " to " + newDef.describeForCompatibility());
            }
        };

        usedColumns.forEach(validateColumn);
        usedColumnArrays.forEach(validateColumn);
    }

    @Override
    public List<String> getColumns() {
        return usedColumns;
    }

    @Override
    public List<String> getColumnArrays() {
        return usedColumnArrays;
    }

    private static Map<String, ColumnDefinition<?>> extractDefinitions(
            Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        final Map<String, ColumnDefinition<?>> result = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : columnsOfInterest.entrySet()) {
            final String name = entry.getKey();
            final Class<?> type = entry.getValue().getType();
            final ColumnDefinition<?> definition =
                    ColumnDefinition.fromGenericType(name, type, entry.getValue().getComponentType());
            result.put(name, definition);
        }
        return result;
    }

    /**
     * Creates a {@link ColumnSource} that will evaluate the result of the {@link #formula} for a given row on demand
     * when it is accessed.
     * <p/>
     * The result of this is the column source produced by calling {@link Table#updateView} or {@link Table#view} on a
     * {@link Table}.
     */
    @NotNull
    @Override
    public ColumnSource<?> getDataView() {
        return getViewColumnSource(false);
    }

    /**
     * Creates a {@link ColumnSource} that will evaluate the result of the {@link #formula} for a given row on demand
     * when it is accessed and cache the result
     *
     * @return the column source produced by calling {@link Table#lazyUpdate} on a {@link Table}.
     */
    @NotNull
    @Override
    public ColumnSource<?> getLazyView() {
        return getViewColumnSource(true);
    }

    @NotNull
    private ColumnSource<?> getViewColumnSource(boolean lazy) {
        final boolean isStateless = isStateless();
        final Formula formula = getFormula(lazy, columnSources, params);
        // noinspection unchecked,rawtypes
        return new ViewColumnSource((returnedType == boolean.class ? Boolean.class : returnedType), formula,
                isStateless);
    }

    private Formula getFormula(boolean initLazyMap,
            Map<String, ? extends ColumnSource<?>> columnsToData,
            QueryScopeParam<?>... params) {
        try {
            // the future must already be completed or else it is an error
            formula = formulaFactoryFuture.get(0, TimeUnit.SECONDS).createFormula(
                    StringEscapeUtils.escapeJava(columnName), rowSet, initLazyMap, columnsToData, params);
        } catch (InterruptedException | TimeoutException e) {
            throw new IllegalStateException("Formula factory not already compiled!");
        } catch (ExecutionException e) {
            throw new UncheckedDeephavenException("Error creating formula for " + columnName, e.getCause());
        }
        return formula;
    }

    @SuppressWarnings("unchecked")
    private static Vector<?> makeAppropriateVectorWrapper(ColumnSource<?> cs, RowSet rowSet) {
        final Class<?> type = cs.getType();
        if (type == Boolean.class) {
            return new ObjectVectorColumnWrapper<>((ColumnSource<Boolean>) cs, rowSet);
        }
        if (type == byte.class) {
            return new ByteVectorColumnWrapper((ColumnSource<Byte>) cs, rowSet);
        }
        if (type == char.class) {
            return new CharVectorColumnWrapper((ColumnSource<Character>) cs, rowSet);
        }
        if (type == double.class) {
            return new DoubleVectorColumnWrapper((ColumnSource<Double>) cs, rowSet);
        }
        if (type == float.class) {
            return new FloatVectorColumnWrapper((ColumnSource<Float>) cs, rowSet);
        }
        if (type == int.class) {
            return new IntVectorColumnWrapper((ColumnSource<Integer>) cs, rowSet);
        }
        if (type == long.class) {
            return new LongVectorColumnWrapper((ColumnSource<Long>) cs, rowSet);
        }
        if (type == short.class) {
            return new ShortVectorColumnWrapper((ColumnSource<Short>) cs, rowSet);
        }
        return new ObjectVectorColumnWrapper<>((ColumnSource<Object>) cs, rowSet);
    }

    protected Future<FormulaFactory> createKernelFormulaFactory(
            @NotNull final CompletionStageFuture<FormulaKernelFactory> formulaKernelFactoryFuture) {
        final FormulaSourceDescriptor sd = getSourceDescriptor();

        return formulaKernelFactoryFuture
                .thenApply(formulaKernelFactory -> (columnName, rowSet, lazy, columnsToData, params) -> {
                    // Maybe warn that we ignore "lazy". By the way, "lazy" is the wrong term anyway. "lazy" doesn't
                    // mean "cached", which is how we are using it.
                    final Map<String, ColumnSource<?>> netColumnSources = new HashMap<>();
                    for (final String sourceColumnName : sd.sources) {
                        final ColumnSource<?> columnSourceToUse = columnsToData.get(sourceColumnName);
                        netColumnSources.put(sourceColumnName, columnSourceToUse);
                    }

                    final Vector<?>[] vectors = new Vector[sd.arrays.length];
                    for (int ii = 0; ii < sd.arrays.length; ++ii) {
                        final ColumnSource<?> cs = columnsToData.get(sd.arrays[ii]);
                        vectors[ii] = makeAppropriateVectorWrapper(cs, rowSet);
                    }
                    final FormulaKernel fk = formulaKernelFactory.createInstance(vectors, params);
                    return new FormulaKernelAdapter(rowSet, sd, netColumnSources, fk);
                });
    }

    protected abstract FormulaSourceDescriptor getSourceDescriptor();

    @Override
    public String getName() {
        return columnName;
    }

    @Override
    public MatchPair getMatchPair() {
        throw new UnsupportedOperationException(
                "Formula " + columnName + " =" + formulaString + " cannot be interpreted as a name value pair");
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean hasVirtualRowVariables() {
        return usesI || usesII || usesK;
    }

    @Override
    public String toString() {
        return formulaString;
    }

    @Override
    public WritableColumnSource<?> newDestInstance(long size) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(rowSet.size(), returnedType);
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(long size) {
        return InMemoryColumnSource.getImmutableMemoryColumnSource(size, returnedType, null);
    }

    static class ColumnArrayParameter {
        final String name;
        final String bareName;
        final Class<?> dataType;
        final Class<?> vectorType;
        final String vectorTypeString;
        final ColumnDefinition<?> columnDefinition;

        public ColumnArrayParameter(String name, String bareName, Class<?> dataType, Class<?> vectorType,
                String vectorTypeString, ColumnDefinition<?> columnDefinition) {
            this.name = name;
            this.bareName = bareName;
            this.dataType = dataType;
            this.vectorType = vectorType;
            this.vectorTypeString = vectorTypeString;
            this.columnDefinition = columnDefinition;
        }
    }

    static class ParamParameter {
        final int index;
        final String name;
        final Class<?> type;
        final String typeString;

        public ParamParameter(int index, String name, Class<?> type, String typeString) {
            this.index = index;
            this.name = name;
            this.type = type;
            this.typeString = typeString;
        }
    }
}
