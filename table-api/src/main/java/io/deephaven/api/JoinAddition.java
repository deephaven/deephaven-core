package io.deephaven.api;

import java.io.Serializable;
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
public interface JoinAddition extends Serializable {

    static JoinAddition of(ColumnName newColumn, ColumnName existingColumn) {
        if (newColumn.equals(existingColumn)) {
            return newColumn;
        }
        return JoinAdditionImpl.of(newColumn, existingColumn);
    }

    static JoinAddition parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            return ColumnName.parse(x);
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

    static List<JoinAddition> from(Collection<String> values) {
        return values.stream().map(JoinAddition::parse).collect(Collectors.toList());
    }

    /**
     * The new column name, to be added to the new table.
     *
     * @return the new column name
     */
    ColumnName newColumn();

    /**
     * The existing column name, from the right table of a join operation.
     *
     * @return the existing column name
     */
    ColumnName existingColumn();
}
