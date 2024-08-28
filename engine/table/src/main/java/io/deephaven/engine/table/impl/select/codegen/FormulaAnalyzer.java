//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.codegen;

import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryLibrary;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.impl.select.QueryScopeParamTypeUtil;
import io.deephaven.time.TimeLiteralReplacedExpression;
import io.deephaven.vector.ObjectVector;
import io.deephaven.engine.table.impl.select.DhFormulaColumn;
import io.deephaven.engine.table.impl.select.formula.FormulaSourceDescriptor;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiConsumer;

import static io.deephaven.engine.table.impl.select.AbstractFormulaColumn.COLUMN_SUFFIX;

public class FormulaAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(FormulaAnalyzer.class);

    /**
     * A container to hold a single copy of imports required to compile formulas for one operation.
     */
    public static final class Imports {
        private final Map<String, Object> queryScopeVariables;
        private final Collection<Package> packageImports;
        private final Collection<Class<?>> classImports;
        private final Collection<Class<?>> staticImports;

        public Imports() {
            final ExecutionContext context = ExecutionContext.getContext();
            queryScopeVariables = Collections.unmodifiableMap(
                    context.getQueryScope().toMap((name, value) -> NameValidator.isValidQueryParameterName(name)));
            final QueryLibrary queryLibrary = context.getQueryLibrary();
            packageImports = Set.copyOf(queryLibrary.getPackageImports());
            classImports = Set.copyOf(queryLibrary.getClassImports());
            staticImports = Set.copyOf(queryLibrary.getStaticImports());
        }

        public Map<String, Object> getQueryScopeVariables() {
            return queryScopeVariables;
        }

        public Collection<Package> getPackageImports() {
            return packageImports;
        }

        public Collection<Class<?>> getClassImports() {
            return classImports;
        }

        public Collection<Class<?>> getStaticImports() {
            return staticImports;
        }
    }

    public static Result analyze(final String rawFormulaString,
            final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            final QueryLanguageParser.Result queryLanguageResult) {

        log.debug().append("Expression (after language conversion) : ")
                .append(queryLanguageResult.getConvertedExpression())
                .append(" isConstantValueExpression=").append(queryLanguageResult.isConstantValueExpression())
                .endl();

        final List<String> usedColumns = new ArrayList<>();
        final List<String> userParams = new ArrayList<>();
        final List<String> usedColumnArrays = new ArrayList<>();
        for (String variable : queryLanguageResult.getVariablesUsed()) {
            final String colSuffix = DhFormulaColumn.COLUMN_SUFFIX;
            final String bareName;
            if (variable.equals("i") || variable.equals("ii") || variable.equals("k")) {
                usedColumns.add(variable);
            } else if (columnDefinitionMap.get(variable) != null) {
                usedColumns.add(variable);
            } else if (variable.endsWith(colSuffix) &&
                    null != columnDefinitionMap
                            .get(bareName = variable.substring(0, variable.length() - colSuffix.length()))) {
                usedColumnArrays.add(bareName);
            } else if (queryLanguageResult.getPossibleParams().containsKey(variable)) {
                userParams.add(variable);
            }
        }
        Class<?> returnedType = queryLanguageResult.getType();
        if (returnedType == boolean.class) {
            returnedType = Boolean.class;
        }
        final String cookedFormulaString = queryLanguageResult.getConvertedExpression();
        final String timeInstanceVariables = queryLanguageResult.getTimeConversionResult().getInstanceVariablesString();
        return new Result(returnedType,
                usedColumns.toArray(String[]::new),
                usedColumnArrays.toArray(String[]::new),
                userParams.toArray(String[]::new),
                rawFormulaString, cookedFormulaString, timeInstanceVariables,
                queryLanguageResult.isConstantValueExpression());
    }

    /**
     * Get the compiled formula for a given formula string.
     *
     * @param formulaString The raw formula string
     * @param availableColumns The columns available for use in the formula
     * @param columnRenames Outer to inner column name mapping
     * @param imports The query scope variables, package, class, and static imports
     * @return The parsed formula {@link QueryLanguageParser.Result result}
     * @throws Exception If the formula cannot be parsed
     */
    public static QueryLanguageParser.Result parseFormula(
            @NotNull final String formulaString,
            @NotNull final Map<String, ColumnDefinition<?>> availableColumns,
            @NotNull final Map<String, String> columnRenames,
            @NotNull final Imports imports) throws Exception {
        return parseFormula(formulaString, availableColumns, columnRenames, imports, true);
    }

    /**
     * Get the compiled formula for a given formula string.
     *
     * @param formulaString The raw formula string
     * @param availableColumns The columns available for use in the formula
     * @param columnRenames Outer to inner column name mapping
     * @param imports The query scope variables, package, class, and static imports
     * @param unboxArguments If true it will unbox the query scope arguments
     * @return The parsed formula {@link QueryLanguageParser.Result result}
     * @throws Exception If the formula cannot be parsed
     */
    public static QueryLanguageParser.Result parseFormula(
            @NotNull final String formulaString,
            @NotNull final Map<String, ColumnDefinition<?>> availableColumns,
            @NotNull final Map<String, String> columnRenames,
            @NotNull final Imports imports,
            final boolean unboxArguments) throws Exception {

        final TimeLiteralReplacedExpression timeConversionResult =
                TimeLiteralReplacedExpression.convertExpression(formulaString);

        final Map<String, Class<?>> possibleVariables = new HashMap<>();
        possibleVariables.put("i", int.class);
        possibleVariables.put("ii", long.class);
        possibleVariables.put("k", long.class);

        final Set<String> columnVariables = new HashSet<>();
        columnVariables.add("i");
        columnVariables.add("ii");
        columnVariables.add("k");

        final Map<String, Class<?>[]> possibleVariableParameterizedTypes = new HashMap<>();

        // Column names get the highest priority.
        final BiConsumer<String, ColumnDefinition<?>> processColumn = (columnName, column) -> {
            if (!columnVariables.add(columnName)) {
                // this column was renamed
                return;
            }

            possibleVariables.put(columnName, column.getDataType());

            final Class<?> compType = column.getComponentType();
            if (compType != null && !compType.isPrimitive()) {
                possibleVariableParameterizedTypes.put(columnName, new Class[] {compType});
            }
        };

        // Renames trump the original columns; so they go first.
        for (Map.Entry<String, String> columnRename : columnRenames.entrySet()) {
            final String columnName = columnRename.getKey();
            final ColumnDefinition<?> column = availableColumns.get(columnRename.getValue());
            processColumn.accept(columnName, column);
        }

        // Now process the original columns.
        for (ColumnDefinition<?> columnDefinition : availableColumns.values()) {
            processColumn.accept(columnDefinition.getName(), columnDefinition);
        }

        // Column arrays come between columns and parameters.
        final BiConsumer<String, ColumnDefinition<?>> processColumnArray = (columnName, column) -> {
            final String columnArrayName = columnName + COLUMN_SUFFIX;

            if (!columnVariables.add(columnArrayName)) {
                // Either this is a rename or overloads an existing column name.
                return;
            }

            final Class<?> vectorType = DhFormulaColumn.getVectorType(column.getDataType());
            possibleVariables.put(columnArrayName, vectorType);

            if (vectorType == ObjectVector.class) {
                possibleVariableParameterizedTypes.put(columnArrayName, new Class[] {column.getDataType()});
            }
        };

        // Renames still trump the original columns; so they go first.
        for (Map.Entry<String, String> columnRename : columnRenames.entrySet()) {
            final String columnName = columnRename.getKey();
            final ColumnDefinition<?> column = availableColumns.get(columnRename.getValue());
            processColumnArray.accept(columnName, column);
        }

        // Now process the original columns.
        for (ColumnDefinition<?> columnDefinition : availableColumns.values()) {
            processColumnArray.accept(columnDefinition.getName(), columnDefinition);
        }

        // Parameters come last.
        for (Map.Entry<String, Object> param : imports.queryScopeVariables.entrySet()) {
            if (possibleVariables.containsKey(param.getKey())) {
                // Columns and column arrays take precedence over parameters.
                continue;
            }

            possibleVariables.put(param.getKey(), QueryScopeParamTypeUtil.getDeclaredClass(param.getValue()));

            Type declaredType = QueryScopeParamTypeUtil.getDeclaredType(param.getValue());
            if (declaredType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) declaredType;
                Class<?>[] paramTypes = Arrays.stream(pt.getActualTypeArguments())
                        .map(QueryScopeParamTypeUtil::classFromType)
                        .toArray(Class<?>[]::new);
                possibleVariableParameterizedTypes.put(param.getKey(), paramTypes);
            }
        }

        log.debug().append("Expression (after time conversion) : ").append(timeConversionResult.getConvertedFormula())
                .endl();

        possibleVariables.putAll(timeConversionResult.getNewVariables());

        return new QueryLanguageParser(timeConversionResult.getConvertedFormula(), imports.getPackageImports(),
                imports.getClassImports(), imports.getStaticImports(), possibleVariables,
                possibleVariableParameterizedTypes, imports.getQueryScopeVariables(), columnVariables, unboxArguments,
                timeConversionResult).getResult();
    }

    public static class Result {
        public final FormulaSourceDescriptor sourceDescriptor;
        public final String rawFormulaString;
        public final String cookedFormulaString;
        public final String timeInstanceVariables;
        public final boolean isConstantValueFormula;

        public Result(Class<?> returnedType, String[] usedColumns, String[] usedArrays, String[] usedParams,
                String rawFormulaString, String cookedFormulaString, String timeInstanceVariables,
                boolean isConstantValueFormula) {
            this.sourceDescriptor = new FormulaSourceDescriptor(returnedType, usedColumns, usedArrays, usedParams);
            this.rawFormulaString = rawFormulaString;
            this.cookedFormulaString = cookedFormulaString;
            this.timeInstanceVariables = timeInstanceVariables;
            this.isConstantValueFormula = isConstantValueFormula;
        }
    }
}
