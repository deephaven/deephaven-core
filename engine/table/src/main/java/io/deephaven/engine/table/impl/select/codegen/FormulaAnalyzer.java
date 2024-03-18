//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.codegen;

import io.deephaven.api.util.NameValidator;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.impl.select.QueryScopeParamTypeUtil;
import io.deephaven.time.TimeLiteralReplacedExpression;
import io.deephaven.vector.ObjectVector;
import io.deephaven.engine.table.impl.select.DhFormulaColumn;
import io.deephaven.engine.table.impl.select.formula.FormulaSourceDescriptor;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

public class FormulaAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(FormulaAnalyzer.class);

    public static Result analyze(final String rawFormulaString,
            final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            final TimeLiteralReplacedExpression timeConversionResult,
            final QueryLanguageParser.Result queryLanguageResult) throws Exception {

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
        final String timeInstanceVariables = timeConversionResult.getInstanceVariablesString();
        return new Result(returnedType,
                usedColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                usedColumnArrays.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                userParams.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                rawFormulaString, cookedFormulaString, timeInstanceVariables,
                queryLanguageResult.isConstantValueExpression());
    }

    public static QueryLanguageParser.Result getCompiledFormula(Map<String, ColumnDefinition<?>> availableColumns,
            TimeLiteralReplacedExpression timeConversionResult) throws Exception {
        final Map<String, Class<?>> possibleVariables = new HashMap<>();
        possibleVariables.put("i", int.class);
        possibleVariables.put("ii", long.class);
        possibleVariables.put("k", long.class);

        final Set<String> columnVariables = new HashSet<>();
        columnVariables.add("i");
        columnVariables.add("ii");
        columnVariables.add("k");

        final Map<String, Class<?>[]> possibleVariableParameterizedTypes = new HashMap<>();

        for (ColumnDefinition<?> columnDefinition : availableColumns.values()) {
            // add column-vectors
            final String columnSuffix = DhFormulaColumn.COLUMN_SUFFIX;
            final Class<?> vectorType = DhFormulaColumn.getVectorType(columnDefinition.getDataType());

            possibleVariables.put(columnDefinition.getName() + columnSuffix, vectorType);
            columnVariables.add(columnDefinition.getName() + columnSuffix);

            if (vectorType == ObjectVector.class) {
                possibleVariableParameterizedTypes.put(columnDefinition.getName() + columnSuffix,
                        new Class[] {columnDefinition.getDataType()});
            }

            // add columns
            columnVariables.add(columnDefinition.getName());
            possibleVariables.put(columnDefinition.getName(), columnDefinition.getDataType());
            final Class<?> compType = columnDefinition.getComponentType();
            if (compType != null && !compType.isPrimitive()) {
                possibleVariableParameterizedTypes.put(columnDefinition.getName(), new Class[] {compType});
            }
        }

        final ExecutionContext context = ExecutionContext.getContext();
        final Map<String, Object> queryScopeVariables = context.getQueryScope().toMap(
                (name, value) -> NameValidator.isValidQueryParameterName(name));
        for (Map.Entry<String, Object> param : queryScopeVariables.entrySet()) {
            if (possibleVariables.containsKey(param.getKey())) {
                // skip any existing matches
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

        // log.debug().append("Expression (before) : ").append(formulaString).endl();

        log.debug().append("Expression (after time conversion) : ").append(timeConversionResult.getConvertedFormula())
                .endl();

        possibleVariables.putAll(timeConversionResult.getNewVariables());

        final Set<Class<?>> classImports =
                new HashSet<>(context.getQueryLibrary().getClassImports());
        classImports.add(TrackingWritableRowSet.class);
        classImports.add(WritableColumnSource.class);
        return new QueryLanguageParser(timeConversionResult.getConvertedFormula(),
                context.getQueryLibrary().getPackageImports(),
                classImports, context.getQueryLibrary().getStaticImports(), possibleVariables,
                possibleVariableParameterizedTypes, queryScopeVariables, columnVariables)
                .getResult();
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
