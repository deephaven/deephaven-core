package io.deephaven.engine.table;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;

/**
 * A descriptor of an input to a multiJoin.
 * <p>
 * The table, key columns, and columns to add are encapsulated in the join descriptor.
 */
@Immutable
@SimpleStyle
public abstract class MultiJoinInput {
    /**
     * Create a multi-join table descriptor.
     *
     * @param inputTable The table to include in a multiJoin
     * @param columnsToMatch An array of {@link JoinMatch} specifying match conditions
     * @param columnsToAdd An array of {@link JoinAddition} specifying the columns from the right side that need to be
     *        added to the table as a result of the match.
     */
    public static MultiJoinInput of(@NotNull final Table inputTable,
            @NotNull JoinMatch[] columnsToMatch,
            @NotNull JoinAddition[] columnsToAdd) {
        return ImmutableMultiJoinInput.of(inputTable, columnsToMatch, columnsToAdd);
    }


    /**
     * Create a multi-join table descriptor.
     *
     * @param inputTable The table to include in a multiJoin
     * @param columnsToMatch A collection of {@link JoinMatch} specifying the key columns
     * @param columnsToAdd A collection of {@link JoinAddition} specifying the columns from the right side that need to
     *        be added to the table as a result of the match.
     */
    public static MultiJoinInput of(@NotNull final Table inputTable,
            @NotNull final Collection<? extends JoinMatch> columnsToMatch,
            @NotNull final Collection<? extends JoinAddition> columnsToAdd) {
        return of(inputTable, columnsToMatch.toArray(JoinMatch[]::new), columnsToAdd.toArray(JoinAddition[]::new));
    }

    /**
     * Create a multi-join table descriptor.
     *
     * @param inputTable The table to include in a multiJoin
     * @param columnsToMatch The key columns, in string format (e.g. "ResultKey=SourceKey" or "KeyInBoth").
     * @param columnsToAdd The columns to add, in string format (e.g. "ResultColumn=SourceColumn" or "ColumnInBoth"),
     *        empty for all columns
     */
    public static MultiJoinInput of(@NotNull final Table inputTable,
            @NotNull final String[] columnsToMatch,
            @NotNull final String[] columnsToAdd) {
        return of(inputTable, JoinMatch.from(columnsToMatch), JoinAddition.from(columnsToAdd));
    }

    /**
     * Create a multi-join table descriptor.
     * <p>
     *
     * @param inputTable the table to include in a multiJoin
     * @param columnsToMatch The match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     */
    public static MultiJoinInput of(@NotNull final Table inputTable, @NotNull final String... columnsToMatch) {
        return of(inputTable, JoinMatch.from(columnsToMatch), Collections.emptyList());
    }

    /**
     * Create a multi-join table descriptor.
     *
     * @param inputTable the table to include in a multiJoin
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the right side that need to be added to the left
     *        side as a result of the match.
     */
    public static MultiJoinInput of(@NotNull final Table inputTable, String columnsToMatch, String columnsToAdd) {
        return of(inputTable,
                columnsToMatch == null || columnsToMatch.isEmpty()
                        ? Collections.emptyList()
                        : JoinMatch.from(columnsToMatch),
                columnsToAdd == null || columnsToAdd.isEmpty()
                        ? Collections.emptyList()
                        : JoinAddition.from(columnsToAdd));
    }

    @Parameter
    public abstract Table inputTable();

    @Parameter
    public abstract JoinMatch[] columnsToMatch();

    @Parameter
    public abstract JoinAddition[] columnsToAdd();
}
