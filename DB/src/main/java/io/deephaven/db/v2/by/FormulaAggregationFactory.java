package io.deephaven.db.v2.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An {@link AggregationContextFactory} used in the implementation of {@link io.deephaven.db.tables.Table#applyToAllBy}.
 */
public class FormulaAggregationFactory implements AggregationContextFactory {

    private final String formula;
    private final String columnParamName;

    public FormulaAggregationFactory(@NotNull final String formula, @NotNull final String columnParamName) {
        this.formula = formula;
        this.columnParamName = columnParamName;
    }

    @Override
    public boolean allowKeyOnlySubstitution() {
        return true;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table inputTable,
            @NotNull final String... groupByColumnNames) {
        final Set<String> groupByColumnNameSet = Arrays.stream(groupByColumnNames).collect(Collectors.toSet());
        final MatchPair[] resultColumns =
                inputTable.getDefinition().getColumnNames().stream().filter(cn -> !groupByColumnNameSet.contains(cn))
                        .map(MatchPairFactory::getExpression).toArray(MatchPair[]::new);

        final ByChunkedOperator byChunkedOperator =
                new ByChunkedOperator((QueryTable) inputTable, false, resultColumns);
        final FormulaChunkedOperator formulaChunkedOperator =
                new FormulaChunkedOperator(byChunkedOperator, true, formula, columnParamName, resultColumns);

        // noinspection unchecked
        return new AggregationContext(
                new IterativeChunkedAggregationOperator[] {formulaChunkedOperator},
                new String[][] {CollectionUtil.ZERO_LENGTH_STRING_ARRAY},
                new ChunkSource.WithPrev[] {null});
    }

    @Override
    public String toString() {
        return "ApplyToAllBy(" + formula + ", " + columnParamName + ')';
    }

    public static QueryTable applyToAllBy(@NotNull final QueryTable inputTable,
            @NotNull final String formula,
            @NotNull final String columnParamName,
            @NotNull final String... groupByColumnNames) {
        return applyToAllBy(AggregationControl.DEFAULT_FOR_OPERATOR, inputTable, formula, columnParamName,
                groupByColumnNames);
    }

    public static QueryTable applyToAllBy(@NotNull final QueryTable inputTable,
            @NotNull final String formula,
            @NotNull final String columnParamName,
            @NotNull final SelectColumn[] groupByColumns) {
        return applyToAllBy(AggregationControl.DEFAULT_FOR_OPERATOR, inputTable, formula, columnParamName,
                groupByColumns);
    }

    public static QueryTable applyToAllBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final String formula,
            @NotNull final String columnParamName,
            @NotNull final String... groupByColumnNames) {
        return applyToAllBy(aggregationControl, inputTable, formula, columnParamName,
                SelectColumnFactory.getExpressions(groupByColumnNames));
    }

    public static QueryTable applyToAllBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final String formula,
            @NotNull final String columnParamName,
            @NotNull final SelectColumn[] groupByColumns) {
        return ChunkedOperatorAggregationHelper.aggregation(aggregationControl,
                new FormulaAggregationFactory(formula, columnParamName), inputTable, groupByColumns);
    }
}
