package io.deephaven.api;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A join addition represents a {@link #newColumn() new column} that should be added as the result
 * of a join, brought over from an {@link #existingColumn() existing column}.
 *
 * @see TableOperations#join(Object, Collection, Collection, int)
 * @see TableOperations#naturalJoin(Object, Collection, Collection)
 * @see TableOperations#exactJoin(Object, Collection, Collection)
 * @see TableOperations#leftJoin(Object, Collection, Collection)
 * @see TableOperations#aj(Object, Collection, Collection, AsOfJoinRule)
 * @see TableOperations#raj(Object, Collection, Collection, ReverseAsOfJoinRule)
 */
@Immutable
@SimpleStyle
public abstract class JoinAddition {

    public static JoinAddition of(ColumnName newAndExisting) {
        return of(newAndExisting, newAndExisting);
    }

    public static JoinAddition of(ColumnName newColumn, ColumnName existingColumn) {
        return ImmutableJoinAddition.of(newColumn, existingColumn);
    }

    public static JoinAddition parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            return of(ColumnName.parse(x));
        }
        final int ix = x.indexOf('=');
        if (ix < 0) {
            throw new IllegalArgumentException(String.format(
                "Unable to parse addition '%s', expected form '<newColumn>=<existingColumn>'", x));
        }
        ColumnName newColumn = ColumnName.parse(x.substring(0, ix));
        ColumnName existingColumn = ColumnName.parse(x.substring(ix + 1));
        return of(newColumn, existingColumn);
    }

    public static List<JoinAddition> from(Collection<String> values) {
        return values.stream().map(JoinAddition::parse).collect(Collectors.toList());
    }

    /**
     * The new column name, to be added to the new table.
     *
     * @return the new column name
     */
    @Parameter
    public abstract ColumnName newColumn();

    /**
     * The existing column name, from the right table of a join operation.
     *
     * @return the existing column name
     */
    @Parameter
    public abstract ColumnName existingColumn();

}
