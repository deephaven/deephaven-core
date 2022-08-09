/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationDescriptions;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.string.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class defines a rollup. It defines both the group-by columns, their order, and all aggregations to be performed
 * on other columns.
 */
public class RollupDefinition {

    private final List<? extends Aggregation> aggregations;
    private final List<? extends ColumnName> groupByColumns;
    private final boolean includeConstituents;
    private final boolean includeOriginalColumns;
    private final boolean includeDescriptions;
    private String name;

    /**
     * Create a RollupDefinition.
     * 
     * @param aggregations the aggregations to perform
     * @param groupByColumns the columns to group by, order matters
     * @param includeConstituents if constituent rows should be included
     * @param includeOriginalColumns if original columns should be included
     * @param includeDescriptions if the rollup should automatically add column descriptions for the chosen aggs
     */
    public RollupDefinition(List<? extends Aggregation> aggregations, Collection<? extends ColumnName> groupByColumns,
            boolean includeConstituents, boolean includeOriginalColumns, boolean includeDescriptions) {
        this(aggregations, groupByColumns, includeConstituents, includeOriginalColumns, includeDescriptions, "");
    }

    /**
     * Create a RollupDefinition.
     * 
     * @param aggregations the aggregations to perform
     * @param groupByColumns the columns to group by, order matters
     * @param includeConstituents if constituent rows should be included
     * @param includeOriginalColumns if original columns should be included
     * @param includeDescriptions if the rollup should automatically add column descriptions for the chosen aggs
     * @param name an optional name.
     */
    public RollupDefinition(List<? extends Aggregation> aggregations, Collection<? extends ColumnName> groupByColumns,
            boolean includeConstituents, boolean includeOriginalColumns, boolean includeDescriptions, String name) {
        this.groupByColumns = new ArrayList<>(groupByColumns);
        this.aggregations = new ArrayList<>(aggregations);
        this.includeConstituents = includeConstituents;
        this.includeOriginalColumns = includeOriginalColumns;
        this.includeDescriptions = includeDescriptions;
        this.name = name;
    }

    /**
     * Create a shallow copy of the specified RollupDefinition
     * 
     * @param other the definition to copy
     */
    public RollupDefinition(RollupDefinition other) {
        this(other.aggregations, other.groupByColumns,
                other.includeConstituents, other.includeOriginalColumns, other.includeDescriptions, other.name);
    }

    /**
     * Get the aggregations and applicable columns for this definition.
     *
     * @return an unmodifiable map of aggregations to set of columns
     */
    public List<? extends Aggregation> getAggregations() {
        return Collections.unmodifiableList(aggregations);
    }

    /**
     * Get the group-by columns for this definition.
     *
     * @return an unmodifiable list of group-by columns
     */
    public List<? extends ColumnName> getGroupByColumns() {
        return Collections.unmodifiableList(groupByColumns);
    }

    /**
     * Check if this definition produces a rollup with constituent columns.
     *
     * @return true if this definition produces rollup constituents
     */
    public boolean includeConstituents() {
        return includeConstituents;
    }

    /**
     * Check if this definition produces a rollup that includes all original columns
     *
     * @return true if this definition includes original columns.
     */
    public boolean includeOriginalColumns() {
        return includeOriginalColumns;
    }

    /**
     * Check if this definition produces a rollup that includes column descriptions for each aggregation.
     *
     * @return true if column descriptions are included.
     */
    public boolean includeDescriptions() {
        return includeDescriptions;
    }

    /**
     * Get the resulting input/output pairs as {@link MatchPair match pairs}.
     *
     * @return The resulting match pairs
     */
    public List<MatchPair> getResultMatchPairs() {
        return AggregationPairs.of(aggregations).map(MatchPair::of).collect(Collectors.toList());
    }

    /**
     * Set the name of this rollup.
     *
     * @param name the name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the name of the rollup.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Apply this rollup definition to the specified table.
     *
     * @param table the table to apply to
     * @return a rollup, as defined by this object.
     */
    public Table applyTo(Table table) {
        Table result = table.rollup(aggregations, includeConstituents, groupByColumns);
        if (includeDescriptions) {
            result = result.withColumnDescription(AggregationDescriptions.of(aggregations));
        }
        return result;
    }

    /**
     * Check equality with another definition. The name is not included in the equality check.
     *
     * @param o the other object
     * @return true if the two definitions describe the same rollup
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RollupDefinition that = (RollupDefinition) o;
        return includeConstituents == that.includeConstituents &&
                includeOriginalColumns == that.includeOriginalColumns &&
                includeDescriptions == that.includeDescriptions &&
                Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(groupByColumns, that.groupByColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregations, groupByColumns,
                includeConstituents, includeOriginalColumns, includeDescriptions);
    }

    public static Builder builder() {
        return new Builder();
    }

    // region Builder for PreDefine

    /**
     * A Builder class to define rollups and attach them to tables as predefined rollups. Instances may be retained to
     * hold references to pre-created rollups.
     */
    public static class Builder {
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private Set<Table> rollupCache;

        private final List<Aggregation> aggregations = new ArrayList<>();
        private List<? extends ColumnName> groupByColumns;
        private boolean includeConstituents;
        private boolean includeOriginalColumns;
        private boolean includeDescriptions;
        private String name;

        /**
         * Set the name for the rollup.
         *
         * @param name the name
         * @return This builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the group-by columns.
         *
         * @param columns the group-by columns
         * @return This builder
         */
        public Builder groupByColumns(String... columns) {
            return groupByColumns(ColumnName.from(columns));
        }

        /**
         * Set the group-by columns.
         *
         * @param columns the group-by columns
         * @return This builder
         */
        public Builder groupByColumns(Collection<? extends ColumnName> columns) {
            groupByColumns = new ArrayList<>(columns);
            return this;
        }

        /**
         * Add an {@link Aggregation aggregation}.
         *
         * @param agg The aggregation
         * @return This builder
         */
        public Builder agg(Aggregation agg) {
            aggregations.add(agg);
            return this;
        }


        /**
         * Set if the result table should include constituents.
         *
         * @param include If constituent rows should be included
         * @return This builder
         */
        public Builder includeConstituents(boolean include) {
            this.includeConstituents = include;
            return this;
        }

        /**
         * Set if the result table should include original columns. NOTE: This is currently unsupported.
         *
         * @param include If original columns should be included
         * @return This builder
         */
        public Builder includeOriginalColumns(boolean include) {
            this.includeOriginalColumns = include;
            return this;
        }

        /**
         * Set if the rollup should include column descriptions for each column.
         *
         * @param includeDescriptions Whether the rollup should add column descriptions
         * @return This builder
         */
        public Builder withDescriptions(boolean includeDescriptions) {
            this.includeDescriptions = includeDescriptions;
            return this;
        }

        /**
         * Create a RollupDefinition from the state of This builder.
         *
         * @return this a new definition.
         * @throws UncheckedDeephavenException if the definition is not complete.
         */
        public RollupDefinition build() throws UncheckedDeephavenException {
            if (StringUtils.isNullOrEmpty(name)) {
                throw new UncheckedDeephavenException("Name not defined for rollup");
            }

            if (groupByColumns == null || groupByColumns.isEmpty()) {
                throw new UncheckedDeephavenException("No group-by columns defined");
            }

            if (aggregations.isEmpty()) {
                throw new UncheckedDeephavenException("No aggregations defined");
            }

            return new RollupDefinition(aggregations, groupByColumns, includeConstituents, includeOriginalColumns,
                    includeDescriptions, name);
        }

        /**
         * Create the rollup definition and attach it to the specified table as a predefined rollup.
         *
         * @param attachTo the table to attach to
         * @return This builder
         */
        public Builder buildAndAttach(Table attachTo) throws UncheckedDeephavenException {
            return buildAndAttach(attachTo, false);
        }

        /**
         * Create the rollup definition and attach it to the specified table as a predefined rollup. Additionally,
         * create and hold a reference to the rollup table if requested.
         *
         * @param attachTo the table to attach to
         * @return This builder
         */
        public Builder buildAndAttach(Table attachTo, boolean preCreate) throws UncheckedDeephavenException {
            final RollupDefinition def = build();

            if (preCreate) {
                if (rollupCache == null) {
                    rollupCache = new HashSet<>();
                }

                rollupCache.add(def.applyTo(attachTo));
            }

            // noinspection unchecked
            List<RollupDefinition> definitionMap =
                    (List<RollupDefinition>) attachTo.getAttribute(Table.PREDEFINED_ROLLUP_ATTRIBUTE);
            if (definitionMap == null) {
                definitionMap = new ArrayList<>();
                attachTo.setAttribute(Table.PREDEFINED_ROLLUP_ATTRIBUTE, definitionMap);
            }

            definitionMap.add(def);
            return this;
        }
    }
    // endregion
}
