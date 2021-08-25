package io.deephaven.treetable;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.by.AggType;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.UncheckedDeephavenException;
import org.jdom2.Element;

import java.io.Serializable;
import java.util.*;

/**
 * This class defines a rollup. It defines both the grouping columns, their order, and all aggregations to be performed
 * on other columns.
 */
public class RollupDefinition implements Serializable {
    private static final long serialVersionUID = 2L;

    public static final String NODE_NAME = "RollupDefinition";
    private static final String GROUP_BY_NODE = "GroupBy";
    private static final String COLUMN_NODE = "Column";
    private static final String OPS_NODE = "Agg";
    private static final String ATTR_NAME = "name";
    private static final String ATTR_CONSTITUENTS = "constituents";
    private static final String ATTR_INCLUDE_OTHER = "includeOther";
    private static final String ATTR_INCLUDE_DESCRIPTIONS = "includeDescriptions";

    private final List<String> groupingColumns;
    private final Map<AggType, Set<String>> aggregations;
    private final boolean includeConstituents;
    private final boolean includeOriginalColumns;
    private final boolean includeDescriptions;
    private String name;

    /**
     * Create a RollupDefinition.
     *
     * @param groupingColumns the columns to group by, order matters
     * @param aggregations the aggregations to perform and which columns to perform them on
     * @param includeConstituents if constituent rows should be included
     * @param includeOriginalColumns if original columns should be included
     * @param includeDescriptions if the rollup should automatically add column descriptions for the chosen aggs
     */
    public RollupDefinition(List<String> groupingColumns, Map<AggType, Set<String>> aggregations,
            boolean includeConstituents, boolean includeOriginalColumns, boolean includeDescriptions) {
        this(groupingColumns, aggregations, includeConstituents, includeOriginalColumns, includeDescriptions, "");
    }

    /**
     * Create a RollupDefinition.
     *
     * @param groupingColumns the columns to group by, order matters
     * @param aggregations the aggregations to perform and which columns to perform them on
     * @param includeConstituents if constituent rows should be included
     * @param includeOriginalColumns if original columns should be included
     * @param includeDescriptions if the rollup should automatically add column descriptions for the chosen aggs
     * @param name an optional name.
     */
    public RollupDefinition(List<String> groupingColumns, Map<AggType, Set<String>> aggregations,
            boolean includeConstituents, boolean includeOriginalColumns, boolean includeDescriptions, String name) {
        this.groupingColumns = new ArrayList<>(groupingColumns);
        this.aggregations = new LinkedHashMap<>();
        aggregations.forEach(
                (agg, cols) -> this.aggregations.computeIfAbsent(agg, a -> new LinkedHashSet<>()).addAll(cols));
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
        this.groupingColumns = new ArrayList<>(other.groupingColumns);
        this.aggregations = new LinkedHashMap<>();
        other.aggregations.forEach(
                (agg, cols) -> this.aggregations.computeIfAbsent(agg, a -> new LinkedHashSet<>()).addAll(cols));
        this.includeConstituents = other.includeConstituents;
        this.includeOriginalColumns = other.includeOriginalColumns;
        this.includeDescriptions = other.includeDescriptions;
        this.name = other.name;
    }

    /**
     * Get the grouping columns for this definition.
     *
     * @return an unmodifiable list of grouping columns
     */
    public List<String> getGroupingColumns() {
        return Collections.unmodifiableList(groupingColumns);
    }

    /**
     * Get the aggregations and applicable columns for this definition.
     *
     * @return an unmodifiable map of aggregations to set of columns
     */
    public Map<AggType, Set<String>> getAggregations() {
        return Collections.unmodifiableMap(aggregations);
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
     * Convert this definition into an XML {@link Element} suitable for use with workspaces.
     *
     * @return an XML representation of this definition
     */
    public Element toXml() {
        final Element info = new Element(NODE_NAME)
                .setAttribute(ATTR_CONSTITUENTS, Boolean.toString(includeConstituents))
                .setAttribute(ATTR_INCLUDE_OTHER, Boolean.toString(includeOriginalColumns))
                .setAttribute(ATTR_INCLUDE_DESCRIPTIONS, Boolean.toString(includeDescriptions));

        if (!StringUtils.isNullOrEmpty(name)) {
            info.setAttribute(ATTR_NAME, name);
        }

        for (final String name : groupingColumns) {
            final Element groupByElem = new Element(GROUP_BY_NODE);
            groupByElem.setAttribute(ATTR_NAME, name);
            info.addContent(groupByElem);
        }

        for (final Map.Entry<AggType, Set<String>> item : aggregations.entrySet()) {
            final Element opElem = new Element(OPS_NODE);
            opElem.setAttribute(ATTR_NAME, item.getKey().toString());

            item.getValue().stream()
                    .map(col -> new Element(COLUMN_NODE).setAttribute(ATTR_NAME, col))
                    .forEach(opElem::addContent);

            info.addContent(opElem);
        }

        return info;
    }

    public List<MatchPair> getResultMatchPairs() {
        final ComboAggregateFactory caf = createComboAggregateFactory(null);
        return caf.getMatchPairs();
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
        final Map<String, String> maybeDescriptions = includeDescriptions ? new HashMap<>() : null;
        Table result =
                table.rollup(createComboAggregateFactory(maybeDescriptions), includeConstituents, groupingColumns);
        if (maybeDescriptions != null) {
            result = result.withColumnDescription(maybeDescriptions);
        }

        return result;
    }

    /**
     * Create the ComboAggregateFactory for this rollup. Generate column descriptions if required.
     *
     * @param descriptions if non-null this method will generate column descriptions
     * @return the ComboAggFactory
     */
    private ComboAggregateFactory createComboAggregateFactory(final Map<String, String> descriptions) {
        final TObjectIntHashMap<String> aggsByColumn = new TObjectIntHashMap<>();
        final List<ComboAggregateFactory.ComboBy> combos = new ArrayList<>(getAggregations().size());

        // Take two passes through the list. The first pass is to decide if we need to append suffixes.
        // The second pass actually creates the aggs.
        for (final Map.Entry<AggType, Set<String>> item : getAggregations().entrySet()) {
            if (item.getKey() != AggType.Count) {
                item.getValue().forEach(c -> aggsByColumn.adjustOrPutValue(c, 1, 1));
            }
        }

        for (final Map.Entry<AggType, Set<String>> item : getAggregations().entrySet()) {
            if (item.getKey() == AggType.Count) {
                combos.add(ComboAggregateFactory.AggCount(item.getValue().stream().findFirst().orElse("Rollup_Count")));
            } else {
                final String[] matchPairs = item.getValue()
                        .stream()
                        .map(col -> {
                            final String aggColName = createAggColName(col, aggsByColumn, item.getKey());
                            if (descriptions != null) {
                                descriptions.put(aggColName, col + " aggregated with " + item.getKey());
                            }

                            return aggColName + "=" + col;
                        }).toArray(String[]::new);
                combos.add(ComboAggregateFactory.Agg(item.getKey(), matchPairs));
            }
        }

        return ComboAggregateFactory.AggCombo(combos.toArray(new ComboAggregateFactory.ComboBy[0]));
    }

    private String createAggColName(String col, TObjectIntHashMap<String> aggsByColumn, AggType agg) {
        return (aggsByColumn.get(col) > 1 && agg != AggType.Sum) || groupingColumns.contains(col) ? col + "_" + agg
                : col;
    }

    /**
     * Create a RollupDefinition from the specified XML element created by {@link #toXml()}.
     *
     * @param rollupElement the element
     * @return a RollupDefinition
     */
    public static RollupDefinition fromXml(Element rollupElement) throws UncheckedDeephavenException {
        final List<String> groupingColumns = new ArrayList<>();
        for (final Element groupByElem : rollupElement.getChildren(GROUP_BY_NODE)) {
            groupingColumns.add(groupByElem.getAttributeValue(ATTR_NAME));
        }

        final Map<AggType, Set<String>> aggs = new LinkedHashMap<>();
        for (final Element opsElem : rollupElement.getChildren(OPS_NODE)) {
            final String name = opsElem.getAttributeValue(ATTR_NAME);
            if (StringUtils.isNullOrEmpty(name)) {
                throw new UncheckedDeephavenException("Rollup element missing attribute name");
            }

            final AggType agg;
            try {
                agg = AggType.valueOf(name);
            } catch (IllegalArgumentException e) {
                throw new UncheckedDeephavenException("Unknown aggregation type:", e);
            }
            final Set<String> colsForAgg = aggs.computeIfAbsent(agg, a -> new LinkedHashSet<>());

            for (final Element colEl : opsElem.getChildren(COLUMN_NODE)) {
                final String colName = colEl.getAttributeValue(ATTR_NAME);
                if (StringUtils.isNullOrEmpty(colName)) {
                    throw new UncheckedDeephavenException("Rollup aggregation column element missing name");
                }

                colsForAgg.add(colName);
            }
        }

        return new RollupDefinition(groupingColumns, aggs,
                Boolean.parseBoolean(rollupElement.getAttributeValue(ATTR_CONSTITUENTS)),
                Boolean.parseBoolean(rollupElement.getAttributeValue(ATTR_INCLUDE_OTHER)),
                Boolean.parseBoolean(rollupElement.getAttributeValue(ATTR_INCLUDE_DESCRIPTIONS)),
                rollupElement.getAttributeValue(ATTR_NAME));
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
                Objects.equals(groupingColumns, that.groupingColumns) &&
                Objects.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupingColumns, aggregations, includeConstituents, includeOriginalColumns);
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

        private List<String> groupingColumns;
        private final Map<AggType, Set<String>> aggregations = new LinkedHashMap<>();
        private boolean includeConstituents;
        private boolean includeOriginalColumns;
        private boolean includeDescriptions;
        private String name;

        /**
         * Set the name for the rollup.
         *
         * @param name the name
         * @return this builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the grouping columns.
         *
         * @param columns the grouping columns
         * @return this builder
         */
        public Builder groupingColumns(String... columns) {
            return groupingColumns(Arrays.asList(columns));
        }

        /**
         * Set the grouping columns.
         *
         * @param columns the grouping columns
         * @return this builder
         */
        public Builder groupingColumns(Collection<String> columns) {
            groupingColumns = new ArrayList<>(columns);
            return this;
        }

        /**
         * Set the columns to include for the specified aggregation.
         *
         * @param type the aggregation
         * @param columns the columns to aggregate
         * @return this builder
         */
        public Builder agg(AggType type, String... columns) {
            if (columns == null || columns.length == 0) {
                aggregations.remove(type);
                return this;
            }

            return agg(type, Arrays.asList(columns));
        }

        /**
         * Set the columns to include for the specified aggregation.
         *
         * @param type the aggregation
         * @param columns the columns to aggregate
         * @return this builder
         */
        public Builder agg(AggType type, Collection<String> columns) {
            if (columns == null || columns.isEmpty()) {
                aggregations.remove(type);
                return this;
            }

            if (type == AggType.Count) {
                if (columns.size() > 1) {
                    throw new IllegalArgumentException("The Count aggregation must have one, and only one column");
                }
            }

            aggregations.computeIfAbsent(type, t -> new LinkedHashSet<>()).addAll(columns);
            return this;
        }

        /**
         * Set if the result table should include constituents. NOTE: This is currently unsupported.
         *
         * @param include if constituent rows should be included
         * @return this builder
         */
        public Builder includeConstituents(boolean include) {
            this.includeConstituents = include;
            return this;
        }

        /**
         * Set if the result table should include original columns. NOTE: This is currently unsupported.
         *
         * @param include if original columns should be included
         * @return this builder
         */
        public Builder includeOriginalColumns(boolean include) {
            this.includeOriginalColumns = include;
            return this;
        }

        /**
         * Set if the rollup should include column descriptions for each column.
         *
         * @param includeDescriptions true if the rollup should add column descriptions
         * @return this builder
         */
        public Builder withDescriptions(boolean includeDescriptions) {
            this.includeDescriptions = includeDescriptions;
            return this;
        }

        /**
         * Create a RollupDefinition from the state of this builder.
         *
         * @return this a new defintion.
         * @throws UncheckedDeephavenException if the definition is not complete.
         */
        public RollupDefinition build() throws UncheckedDeephavenException {
            if (StringUtils.isNullOrEmpty(name)) {
                throw new UncheckedDeephavenException("Name not defined for rollup");
            }

            if (groupingColumns == null || groupingColumns.isEmpty()) {
                throw new UncheckedDeephavenException("No grouping columns defined");
            }

            if (aggregations.isEmpty()) {
                throw new UncheckedDeephavenException("No aggregations defined");
            }

            return new RollupDefinition(groupingColumns, aggregations, includeConstituents, includeOriginalColumns,
                    includeDescriptions, name);
        }

        /**
         * Create the rollup definition and attach it to the specified table as a predefined rollup.
         *
         * @param attachTo the table to attach to
         * @return this builder
         * @throws UncheckedDeephavenException
         */
        public Builder buildAndAttach(Table attachTo) throws UncheckedDeephavenException {
            return buildAndAttach(attachTo, false);
        }

        /**
         * Create the rollup definition and attach it to the specified table as a predefined rollup. Additionally,
         * create and hold a reference to the rollup table if requested.
         *
         * @param attachTo the table to attach to
         * @return this builder
         * @throws UncheckedDeephavenException
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
