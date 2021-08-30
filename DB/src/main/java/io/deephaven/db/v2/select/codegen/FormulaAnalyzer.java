package io.deephaven.db.v2.select.codegen;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.lang.DBLanguageParser;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.select.Param;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.select.DhFormulaColumn;
import io.deephaven.db.v2.select.FormulaCompilationException;
import io.deephaven.db.v2.select.formula.FormulaSourceDescriptor;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.util.*;

public class FormulaAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(FormulaAnalyzer.class);

    public static Result analyze(final String rawFormulaString,
            final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            Map<String, Class<?>> otherVariables) {
        try {
            return analyzeHelper(rawFormulaString, columnDefinitionMap, otherVariables);
        } catch (Exception e) {
            throw new FormulaCompilationException("Formula compilation error for: " + rawFormulaString, e);
        }
    }

    private static Result analyzeHelper(final String rawFormulaString,
            final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            Map<String, Class<?>> otherVariables) throws Exception {
        final Map<String, Param<?>> possibleParams = new HashMap<>();
        final QueryScope queryScope = QueryScope.getScope();
        for (Param<?> param : queryScope.getParams(queryScope.getParamNames())) {
            possibleParams.put(param.getName(), param);
        }

        final DBTimeUtils.Result timeConversionResult = DBTimeUtils.convertExpression(rawFormulaString);
        final DBLanguageParser.Result result = getCompiledFormula(columnDefinitionMap, timeConversionResult,
                otherVariables);

        log.debug().append("Expression (after language conversion) : ").append(result.getConvertedExpression()).endl();

        final List<String> usedColumns = new ArrayList<>();
        final List<String> userParams = new ArrayList<>();
        final List<String> usedColumnArrays = new ArrayList<>();
        for (String variable : result.getVariablesUsed()) {
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
            } else if (possibleParams.containsKey(variable)) {
                userParams.add(variable);
            }
        }
        Class<?> returnedType = result.getType();
        if (returnedType == boolean.class) {
            returnedType = Boolean.class;
        }
        final String cookedFormulaString = result.getConvertedExpression();
        final String timeInstanceVariables = timeConversionResult.getInstanceVariablesString();
        return new Result(returnedType,
                usedColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                usedColumnArrays.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                userParams.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                rawFormulaString, cookedFormulaString, timeInstanceVariables);
    }

    public static DBLanguageParser.Result getCompiledFormula(Map<String, ColumnDefinition<?>> availableColumns,
            DBTimeUtils.Result timeConversionResult,
            Map<String, Class<?>> otherVariables) throws Exception {
        final Map<String, Class<?>> possibleVariables = new HashMap<>();
        possibleVariables.put("i", int.class);
        possibleVariables.put("ii", long.class);
        possibleVariables.put("k", long.class);

        final Map<String, Class<?>[]> possibleVariableParameterizedTypes = new HashMap<>();

        for (ColumnDefinition<?> columnDefinition : availableColumns.values()) {
            final String columnSuffix = DhFormulaColumn.COLUMN_SUFFIX;
            final Class<?> dbArrayType = DhFormulaColumn.getDbArrayType(columnDefinition.getDataType());

            possibleVariables.put(columnDefinition.getName() + columnSuffix, dbArrayType);

            if (dbArrayType == DbArray.class) {
                possibleVariableParameterizedTypes.put(columnDefinition.getName() + columnSuffix,
                        new Class[] {columnDefinition.getDataType()});
            }
        }

        final QueryScope queryScope = QueryScope.getScope();
        for (Param<?> param : queryScope.getParams(queryScope.getParamNames())) {
            possibleVariables.put(param.getName(), param.getDeclaredType());
        }

        for (ColumnDefinition<?> columnDefinition : availableColumns.values()) {
            possibleVariables.put(columnDefinition.getName(), columnDefinition.getDataType());
            final Class<?> compType = columnDefinition.getComponentType();
            if (compType != null && !compType.isPrimitive()) {
                possibleVariableParameterizedTypes.put(columnDefinition.getName(), new Class[] {compType});
            }
        }

        // log.debug().append("Expression (before) : ").append(formulaString).endl();

        log.debug().append("Expression (after time conversion) : ").append(timeConversionResult.getConvertedFormula())
                .endl();

        possibleVariables.putAll(timeConversionResult.getNewVariables());
        if (otherVariables != null) {
            possibleVariables.putAll(otherVariables);
        }

        final Set<Class<?>> classImports = new HashSet<>(QueryLibrary.getClassImports());
        classImports.add(Index.class);
        classImports.add(WritableSource.class);
        return new DBLanguageParser(timeConversionResult.getConvertedFormula(), QueryLibrary.getPackageImports(),
                classImports, QueryLibrary.getStaticImports(), possibleVariables, possibleVariableParameterizedTypes)
                        .getResult();
    }

    public static class Result {
        public final FormulaSourceDescriptor sourceDescriptor;
        public final String rawFormulaString;
        public final String cookedFormulaString;
        public final String timeInstanceVariables;

        public Result(Class<?> returnedType, String[] usedColumns, String[] usedArrays, String[] usedParams,
                String rawFormulaString, String cookedFormulaString, String timeInstanceVariables) {
            this.sourceDescriptor = new FormulaSourceDescriptor(returnedType, usedColumns, usedArrays, usedParams);
            this.rawFormulaString = rawFormulaString;
            this.cookedFormulaString = cookedFormulaString;
            this.timeInstanceVariables = timeInstanceVariables;
        }
    }
}
