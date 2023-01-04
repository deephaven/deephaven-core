package io.deephaven.api.snapshot;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The options for creating a snapshotting table with respect to the {@code base} and {@code trigger} tables of
 * {@link io.deephaven.api.TableOperations#snapshotWhen(Object, SnapshotWhenOptions)}.
 */
@Immutable
@BuildableStyle
public abstract class SnapshotWhenOptions {

    public enum Feature {
        /**
         * Whether to take an initial snapshot upon construction.
         */
        INITIAL,
        /**
         * Whether the snapshot should be incremental.
         */
        INCREMENTAL,
        /**
         * Whether the snapshot should keep history.
         */
        HISTORY
    }

    public static Builder builder() {
        return ImmutableSnapshotWhenOptions.builder();
    }

    /**
     * Creates an options with the {@code features}.
     *
     * <p>
     * Equivalent to {@code builder().addFlags(features).build()}.
     *
     * @param features the features
     * @return the snapshot control
     */
    public static SnapshotWhenOptions of(Feature... features) {
        return builder().addFlags(features).build();
    }

    /**
     * Creates an options with the features and columns.
     *
     * @param initial for {@link Feature#INITIAL}
     * @param incremental for {@link Feature#INCREMENTAL}
     * @param history for {@link Feature#HISTORY}
     * @param stampColumns the stamp columns
     * @return the options
     */
    public static SnapshotWhenOptions of(boolean initial, boolean incremental, boolean history,
            String... stampColumns) {
        final Builder builder = builder();
        if (initial) {
            builder.addFlags(Feature.INITIAL);
        }
        if (incremental) {
            builder.addFlags(Feature.INCREMENTAL);
        }
        if (history) {
            builder.addFlags(Feature.HISTORY);
        }
        for (String stampColumn : stampColumns) {
            builder.addStampColumns(ColumnName.of(stampColumn));
        }
        return builder.build();
    }

    /**
     * Creates an options containing the {@code features} and {@code stampColumns}.
     *
     * @param features the features
     * @param stampColumns the stamp columns
     * @return the snapshot control
     * @see ColumnName#of(String)
     */
    public static SnapshotWhenOptions of(Iterable<Feature> features, String... stampColumns) {
        final Builder builder = builder().addAllFlags(features);
        for (String stampColumn : stampColumns) {
            builder.addStampColumns(ColumnName.of(stampColumn));
        }
        return builder.build();
    }

    /**
     * The feature flags.
     */
    public abstract Set<Feature> flags();

    /**
     * The columns to stamp from the {@code trigger} table. An empty list means stamp all.
     */
    public abstract List<ColumnName> stampColumns();

    // Note: should there be a feature to specify that empty means NO stamp columns?
    // Engine doesn't currently support it, but it's a legitimate operation.

    /**
     * Check if the {@code feature} is set.
     *
     * <p>
     * Equivalent to {@code flags().contains(feature)}.
     *
     * @param feature the feature
     * @return true if {@code this} has {@code feature}
     */
    public final boolean has(Feature feature) {
        return flags().contains(feature);
    }

    /**
     * Creates a description of the options.
     *
     * @return the description
     */
    public final String description() {
        return String.format("initial=%b,incremental=%b,history=%b,stampColumns=%s",
                has(Feature.INITIAL),
                has(Feature.INCREMENTAL),
                has(Feature.HISTORY),
                stampColumns().stream().map(ColumnName::name).collect(Collectors.joining(",", "[", "]")));
    }

    public interface Builder {
        Builder addStampColumns(ColumnName element);

        Builder addStampColumns(ColumnName... elements);

        Builder addAllStampColumns(Iterable<? extends ColumnName> elements);

        Builder addFlags(Feature element);

        Builder addFlags(Feature... elements);

        Builder addAllFlags(Iterable<Feature> elements);

        SnapshotWhenOptions build();
    }
}
