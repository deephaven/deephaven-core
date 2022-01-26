/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.table.impl.vector.*;
import io.deephaven.engine.table.impl.select.formula.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.api.util.NameValidator;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.*;
import java.util.*;

/**
 * A SelectColumn that implements a formula computed from the existing columns in the table and a query scope.
 */
public abstract class AbstractFormulaColumn implements FormulaColumn {
    private static final Logger log = LoggerFactory.getLogger(AbstractFormulaColumn.class);

    private final boolean useKernelFormulas;


    private static final boolean ALLOW_UNSAFE_REFRESHING_FORMULAS = Configuration.getInstance()
            .getBooleanForClassWithDefault(AbstractFormulaColumn.class, "allowUnsafeRefreshingFormulas", false);


    protected String formulaString;
    protected List<String> usedColumns;

    @NotNull
    protected final String columnName;
    private FormulaFactory formulaFactory;
    private Formula formula;
    protected List<String> userParams;
    protected QueryScopeParam<?>[] params;
    protected Map<String, ? extends ColumnSource<?>> columnSources;
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
     * @param useKernelFormulas
     */
    protected AbstractFormulaColumn(String columnName, String formulaString, boolean useKernelFormulas) {
        this.formulaString = Require.neqNull(formulaString, "formulaString");
        this.columnName = NameValidator.validateColumnName(columnName);
        this.useKernelFormulas = useKernelFormulas;
    }

    @Override
    public List<String> initInputs(Table table) {
        return initInputs(table.getRowSet(), table.getColumnSourceMap());
    }

    @Override
    public Class<?> getReturnedType() {
        return returnedType;
    }

    @Override
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        if (this.rowSet != null) {
            Assert.eq(this.rowSet, "this.rowSet", rowSet, "rowSet");
        }
        this.rowSet = rowSet;

        this.columnSources = columnsOfInterest;
        if (usedColumns != null) {
            return usedColumns;
        }
        return initDef(extractDefinitions(columnsOfInterest));
    }

    protected void applyUsedVariables(Map<String, ColumnDefinition<?>> columnDefinitionMap, Set<String> variablesUsed) {
        final Map<String, QueryScopeParam<?>> possibleParams = new HashMap<>();
        final QueryScope queryScope = QueryScope.getScope();
        for (QueryScopeParam<?> param : queryScope.getParams(queryScope.getParamNames())) {
            possibleParams.put(param.getName(), param);
        }

        final List<QueryScopeParam<?>> paramsList = new ArrayList<>();
        usedColumns = new ArrayList<>();
        userParams = new ArrayList<>();
        usedColumnArrays = new ArrayList<>();
        for (String variable : variablesUsed) {
            if (variable.equals("i")) {
                usesI = true;
            } else if (variable.equals("ii")) {
                usesII = true;
            } else if (variable.equals("k")) {
                usesK = true;
            } else if (columnDefinitionMap.get(variable) != null) {
                usedColumns.add(variable);
            } else {
                String strippedColumnName =
                        variable.substring(0, Math.max(0, variable.length() - COLUMN_SUFFIX.length()));
                if (variable.endsWith(COLUMN_SUFFIX) && columnDefinitionMap.get(strippedColumnName) != null) {
                    usedColumnArrays.add(strippedColumnName);
                } else if (possibleParams.containsKey(variable)) {
                    paramsList.add(possibleParams.get(variable));
                    userParams.add(variable);
                }
            }
        }

        params = paramsList.toArray(QueryScopeParam.ZERO_LENGTH_PARAM_ARRAY);
        for (QueryScopeParam<?> param : paramsList) {
            try {
                // noinspection ResultOfMethodCallIgnored, we only care that we can get the value here not what it is
                param.getValue();
            } catch (RuntimeException e) {
                throw new RuntimeException("Error retrieving " + param.getName(), e);
            }
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
        final boolean preventsParallelization = preventsParallelization();
        final boolean isStateless = isStateless();

        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // We explicitly want all Groovy commands to run under the 'file:/groovy/shell' source, so explicitly create
            // that.
            AccessControlContext context;
            try {
                final URL urlSource = new URL("file:/groovy/shell");
                final CodeSource codeSource = new CodeSource(urlSource, (java.security.cert.Certificate[]) null);
                final PermissionCollection perms = Policy.getPolicy().getPermissions(codeSource);
                context = new AccessControlContext(new ProtectionDomain[] {new ProtectionDomain(codeSource, perms)});
            } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid file path in groovy url source", e);
            }

            return AccessController.doPrivileged((PrivilegedAction<ColumnSource<?>>) () -> {
                final Formula formula = getFormula(lazy, columnSources, params);
                // noinspection unchecked,rawtypes
                return new ViewColumnSource((returnedType == boolean.class ? Boolean.class : returnedType), formula,
                        preventsParallelization, isStateless);
            }, context);
        } else {
            final Formula formula = getFormula(lazy, columnSources, params);
            // noinspection unchecked,rawtypes
            return new ViewColumnSource((returnedType == boolean.class ? Boolean.class : returnedType), formula,
                    preventsParallelization, isStateless);
        }
    }

    public abstract boolean preventsParallelization();

    private Formula getFormula(boolean initLazyMap,
            Map<String, ? extends ColumnSource<?>> columnsToData,
            QueryScopeParam<?>... params) {
        if (formulaFactory == null) {
            formulaFactory = useKernelFormulas ? createKernelFormulaFactory() : createFormulaFactory();
        }
        formula = formulaFactory.createFormula(rowSet, initLazyMap, columnsToData, params);
        return formula;
    }

    protected FormulaFactory createFormulaFactory() {
        throw new UnsupportedOperationException();
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

    private FormulaFactory createKernelFormulaFactory() {
        final FormulaKernelFactory formulaKernelFactory = getFormulaKernelFactory();
        final FormulaSourceDescriptor sd = getSourceDescriptor();

        return (rowSet, lazy, columnsToData, params) -> {
            // Maybe warn that we ignore "lazy". By the way, "lazy" is the wrong term anyway. "lazy" doesn't mean
            // "cached", which is how we are using it.
            final Map<String, ColumnSource<?>> netColumnSources = new HashMap<>();
            for (final String columnName : sd.sources) {
                final ColumnSource<?> columnSourceToUse = columnsToData.get(columnName);
                netColumnSources.put(columnName, columnSourceToUse);
            }

            final Vector<?>[] vectors = new Vector[sd.arrays.length];
            for (int ii = 0; ii < sd.arrays.length; ++ii) {
                final ColumnSource<?> cs = columnsToData.get(sd.arrays[ii]);
                vectors[ii] = makeAppropriateVectorWrapper(cs, rowSet);
            }
            final FormulaKernel fk = formulaKernelFactory.createInstance(vectors, params);
            return new FormulaKernelAdapter(rowSet, sd, netColumnSources, fk);
        };
    }

    protected abstract FormulaSourceDescriptor getSourceDescriptor();

    protected abstract FormulaKernelFactory getFormulaKernelFactory();

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

    @Override
    public boolean disallowRefresh() {
        return !ALLOW_UNSAFE_REFRESHING_FORMULAS && !usesI && !usesII && !usesK && usedColumnArrays.isEmpty();
    }

    static class ColumnArrayParameter {
        final String name;
        final String bareName;
        final Class<?> dataType;
        final Class<?> vectorType;
        final String vectorTypeString;
        final ColumnSource<?> columnSource;

        public ColumnArrayParameter(String name, String bareName, Class<?> dataType, Class<?> vectorType,
                String vectorTypeString, ColumnSource<?> columnSource) {
            this.name = name;
            this.bareName = bareName;
            this.dataType = dataType;
            this.vectorType = vectorType;
            this.vectorTypeString = vectorTypeString;
            this.columnSource = columnSource;
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
