package io.deephaven.engine.table;

import io.deephaven.api.JoinMatch;
import org.jetbrains.annotations.NotNull;

import java.util.ServiceLoader;

/**
 * <p>
 * Join unique rows from a set of tables onto a set of common keys.
 * </p>
 *
 * <p>
 * The multiJoin operation collects the set of distinct keys from the input tables, then joins at most one row per key
 * from each of the input tables onto the result. Input tables need not have a matching row for each key, but they may
 * not have multiple matching rows for a given key.
 * </p>
 *
 * <p>
 * Input tables with non-matching key column names must use the {@link JoinMatch} format to map keys to the common
 * output table key column names (e.g. "OutputKey=SourceKey"). Also, individual columns to include from input tables may
 * be specified and optionally renamed using {@link io.deephaven.api.JoinAddition} format (e.g. "NewCol=OldColName"). If
 * no output columns are specified then every non-key column from the input table will be included in the multi-join
 * output table.
 * </p>
 *
 * <p>
 * The multiJoin operation can be thought of as a merge of the key columns, followed by a selectDistinct and then a
 * series of iterative naturalJoin operations as follows (this example has common key column names and includes all
 * columns from the input tables):
 * </p>
 *
 * <pre>{@code
 *     private Table doIterativeMultiJoin(String [] keyColumns, List<? extends Table> inputTables) {
 *         final List<Table> keyTables = inputTables.stream().map(t -> t.view(keyColumns)).collect(Collectors.toList());
 *         final Table base = TableTools.merge(keyTables).selectDistinct(keyColumns);
 *
 *         Table result = base;
 *         for (int ii = 0; ii < inputTables.size(); ++ii) {
 *             result = result.naturalJoin(inputTables.get(ii), Arrays.asList(keyColumns));
 *         }
 *
 *         return result;
 *     }
 *     }
 * </pre>
 */

public class MultiJoinFactory {

    /**
     * Creator interface for runtime-supplied implementation.
     */
    public interface Creator {
        MultiJoinTable of(@NotNull final MultiJoinInput... multiJoinInputs);
    }

    /**
     * Creator provider to supply the implementation at runtime.
     */
    @FunctionalInterface
    public interface CreatorProvider {
        Creator get();
    }

    private static final class MultiJoinTableCreatorHolder {
        private static final MultiJoinFactory.Creator creator =
                ServiceLoader.load(MultiJoinFactory.CreatorProvider.class).iterator().next().get();
    }

    private static MultiJoinFactory.Creator multiJoinTableCreator() {
        return MultiJoinTableCreatorHolder.creator;
    }

    /**
     * Join tables that have common key column names; include all columns from the input tables.
     * <p>
     *
     * @param keys the key column pairs in the format "Result=Source" or "ColumnInBoth"
     * @param inputTables the tables to join together
     * @return a MultiJoinTable with one row for each key and the corresponding row in each input table
     */
    public static MultiJoinTable of(@NotNull final String[] keys, @NotNull final Table... inputTables) {
        return multiJoinTableCreator().of(MultiJoinInput.from(keys, inputTables));
    }

    /**
     * Join tables that have common key column names; include all columns from the input tables.
     * <p>
     *
     * @param columnsToMatch A comma separated list of key columns, in string format (e.g. "ResultKey=SourceKey" or
     *        "KeyInBoth").
     * @param inputTables the tables to join together
     * @return a MultiJoinTable with one row for each key and the corresponding row in each input table
     */
    public static MultiJoinTable of(@NotNull final String columnsToMatch, @NotNull final Table... inputTables) {
        return multiJoinTableCreator().of(MultiJoinInput.from(columnsToMatch, inputTables));
    }

    /**
     * Perform a multiJoin for one or more tables; allows renaming of key column names and specifying individual input
     * table columns to include in the final output table.
     *
     * @param multiJoinInputs the description of each table that contributes to the result
     * @return a MultiJoinTable with one row for each key and the corresponding row in each input table
     */
    public static MultiJoinTable of(@NotNull final MultiJoinInput... multiJoinInputs) {
        return multiJoinTableCreator().of(multiJoinInputs);
    }
}
