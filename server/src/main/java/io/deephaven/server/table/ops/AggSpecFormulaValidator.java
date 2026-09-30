//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.FormulaUtil;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.validation.ColumnExpressionValidator;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecFormula;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared helper that runs an {@link AggSpecFormula} through the {@link ColumnExpressionValidator} for every input
 * column the engine will compile the formula against at runtime. The substitution performed here matches
 * {@code FormulaChunkedOperator}'s use of {@link FormulaUtil#replaceFormulaTokens(String, String, String)}, so the
 * validator inspects the exact string the engine would compile.
 */
final class AggSpecFormulaValidator {

    private AggSpecFormulaValidator() {}

    /**
     * Validate {@code formula} for each column in {@code inputColumnNames}, substituting the formula's paramToken with
     * the column name.
     *
     * @param formula the grpc {@link AggSpecFormula}
     * @param parent the parent table whose definition is the source shape
     * @param groupByColumns the group-by columns (used to build a grouped prototype so non-key columns appear as
     *        vectors, matching the post-{@code groupBy} shape the engine sees)
     * @param inputColumnNames the names of the columns the formula will be applied to
     * @param validator the column expression validator
     */
    static void validate(
            final AggSpecFormula formula,
            final Table parent,
            final List<ColumnName> groupByColumns,
            final List<String> inputColumnNames,
            final ColumnExpressionValidator validator) {
        if (inputColumnNames.isEmpty()) {
            return;
        }
        final Table groupedPrototype =
                TableTools.newTable(parent.getDefinition()).groupBy(groupByColumns);
        final String formulaString = formula.getFormula();
        final String paramToken = formula.getParamToken();
        final String[] expressions = inputColumnNames.stream()
                .map(columnName -> columnName + "="
                        + FormulaUtil.replaceFormulaTokens(formulaString, paramToken, columnName))
                .toArray(String[]::new);
        final SelectColumn[] selectColumns = SelectColumn.from(Selectable.from(expressions));
        validator.validateColumnExpressions(selectColumns, expressions, groupedPrototype.getDefinition());
    }

    /**
     * Compute the input column names for the {@code aggAllBy} path: every column in the parent table definition that
     * isn't a group-by column. For {@link AggSpecFormula} this matches {@code AggregateAllExclusions} (which adds no
     * further exclusions for formula specs).
     */
    static List<String> nonKeyColumnNames(final Table parent, final List<ColumnName> groupByColumns) {
        final Set<String> keyNames = groupByColumns.stream()
                .map(ColumnName::name)
                .collect(Collectors.toSet());
        return parent.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(name -> !keyNames.contains(name))
                .collect(Collectors.toList());
    }
}
