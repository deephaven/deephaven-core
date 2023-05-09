package io.deephaven.api.snapshot;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.Strings;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Set;

/**
 * The options for creating a snapshotting table with respect to the {@code base} and {@code trigger} tables of
 * {@link io.deephaven.api.TableOperations#snapshotWhen(Object, SnapshotWhenOptions)}.
 *
 * <p>
 * When {@code trigger} updates, a snapshot of {@code base} and the "stamp key" from {@code trigger} form the resulting
 * table. The "stamp key" is the last row of the {@code trigger} table, limited by the {@link #stampColumns() stamp
 * columns}. If {@code trigger} is empty, the "stamp key" will be represented by {@code null} values.
 */
@Immutable
@BuildableStyle
public abstract class SnapshotWhenOptions {

    public enum Flag {
        /**
         * Whether to take an initial snapshot upon construction. When not specified, the resulting table will remain
         * empty until {@code trigger} first updates.
         */
        INITIAL,
        /**
         * Whether the resulting table should be incremental. When incremental, only the rows of {@code base} that have
         * been added or updated will have the latest "stamp key".
         */
        INCREMENTAL,
        /**
         * Whether the resulting table should keep history. A history table appends a full snapshot of {@code base} and
         * the "stamp key" as opposed to updating existing rows.
         *
         * <p>
         * Note: this flag is currently incompatible with {@link #INITIAL} and {@link #INCREMENTAL}.
         *
         * @see <a href="https://github.com/deephaven/deephaven-core/issues/3260">deephaven-core#3260</a>
         */
        HISTORY
    }

    public static Builder builder() {
        return ImmutableSnapshotWhenOptions.builder();
    }

    /**
     * Creates an options with the {@code flags}.
     *
     * <p>
     * Equivalent to {@code builder().addFlags(flags).build()}.
     *
     * @param flags the flags
     * @return the snapshot control
     */
    public static SnapshotWhenOptions of(Flag... flags) {
        return builder().addFlags(flags).build();
    }

    /**
     * Creates an options with the flags and {@code stampColumns}..
     *
     * @param initial for {@link Flag#INITIAL}
     * @param incremental for {@link Flag#INCREMENTAL}
     * @param history for {@link Flag#HISTORY}
     * @param stampColumns the stamp columns
     * @return the options
     */
    public static SnapshotWhenOptions of(boolean initial, boolean incremental, boolean history,
            String... stampColumns) {
        final Builder builder = builder();
        if (initial) {
            builder.addFlags(Flag.INITIAL);
        }
        if (incremental) {
            builder.addFlags(Flag.INCREMENTAL);
        }
        if (history) {
            builder.addFlags(Flag.HISTORY);
        }
        for (String stampColumn : stampColumns) {
            builder.addStampColumns(JoinAddition.parse(stampColumn));
        }
        return builder.build();
    }

    /**
     * Creates an options containing the {@code flags} and {@code stampColumns}.
     *
     * @param flags the flags
     * @param stampColumns the stamp columns
     * @return the snapshot control
     * @see ColumnName#of(String)
     */
    public static SnapshotWhenOptions of(Iterable<Flag> flags, String... stampColumns) {
        final Builder builder = builder().addAllFlags(flags);
        for (String stampColumn : stampColumns) {
            builder.addStampColumns(JoinAddition.parse(stampColumn));
        }
        return builder.build();
    }

    /**
     * The feature flags.
     */
    public abstract Set<Flag> flags();

    /**
     * The {@code trigger} table stamp columns. If empty, it represents all columns from the {@code trigger} table.
     */
    public abstract List<JoinAddition> stampColumns();

    /**
     * Check if the {@code flag} is set.
     *
     * <p>
     * Equivalent to {@code flags().contains(flag)}.
     *
     * @param flag the flag
     * @return true if {@code this} has {@code flag}
     */
    public final boolean has(Flag flag) {
        return flags().contains(flag);
    }

    /**
     * Creates a description of the options.
     *
     * @return the description
     */
    public final String description() {
        return String.format("initial=%b,incremental=%b,history=%b,stampColumns=%s",
                has(Flag.INITIAL),
                has(Flag.INCREMENTAL),
                has(Flag.HISTORY),
                Strings.ofJoinAdditions(stampColumns()));
    }

    @Check
    final void checkHistory() {
        if (has(Flag.HISTORY) && (has(Flag.INCREMENTAL) || has(Flag.INITIAL))) {
            throw new UnsupportedOperationException(
                    "snapshotWhen with history does not currently support incremental nor initial. See https://github.com/deephaven/deephaven-core/issues/3260.");
        }
    }

    public interface Builder {
        Builder addStampColumns(JoinAddition stampColumn);

        Builder addStampColumns(JoinAddition... stampColumns);

        Builder addAllStampColumns(Iterable<? extends JoinAddition> stampColumns);

        Builder addFlags(Flag flag);

        Builder addFlags(Flag... flags);

        Builder addAllFlags(Iterable<Flag> flags);

        SnapshotWhenOptions build();
    }
}
