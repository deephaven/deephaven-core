package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.TimeZone;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link PartitionedTable.Proxy} implementation.
 */
class PartitionedTableProxyImpl implements PartitionedTable.Proxy {

    /**
     * Make a {@link PartitionedTable.Proxy proxy} to the supplied {@code target}.
     *
     * @param target The target {@link PartitionedTable} whose constituents should be used when proxying
     *        {@link TableOperations table operations}
     * @param requireMatchingKeys As in {@link PartitionedTable#proxy(boolean, boolean)}
     * @param sanityCheckJoins As in {@link PartitionedTable#proxy(boolean, boolean)}
     * @return A {@link PartitionedTable.Proxy proxy} to {@code target}
     */
    static PartitionedTable.Proxy of(
            @NotNull final PartitionedTable target,
            final boolean requireMatchingKeys,
            final boolean sanityCheckJoins) {
        return new PartitionedTableProxyImpl(target, requireMatchingKeys, sanityCheckJoins);
    }

    private static final ColumnName FOUND_IN = ColumnName.of("__FOUND_IN__");
    private static final ColumnName ENCLOSING_CONSTITUENT = ColumnName.of("__ENCLOSING_CONSTITUENT__");

    /**
     * The underlying target {@link PartitionedTable}.
     */
    private final PartitionedTable target;

    /**
     * Whether to require that partitioned table arguments used in multi-table operations have matching key sets.
     */
    private final boolean requireMatchingKeys;

    /**
     * Whether to check that join operations don't have overlapping keys in any constituents of their partitioned table
     * inputs.
     */
    private final boolean sanityCheckJoins;

    private PartitionedTableProxyImpl(
            @NotNull final PartitionedTable target,
            final boolean requireMatchingKeys,
            final boolean sanityCheckJoins) {
        this.target = target;
        this.requireMatchingKeys = requireMatchingKeys;
        this.sanityCheckJoins = sanityCheckJoins;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final PartitionedTableProxyImpl that = (PartitionedTableProxyImpl) other;
        return requireMatchingKeys == that.requireMatchingKeys
                && sanityCheckJoins == that.sanityCheckJoins
                && target.equals(that.target);
    }

    @Override
    public int hashCode() {
        int result = 31;
        result = 31 * result + target.hashCode();
        result = 31 * result + Boolean.hashCode(requireMatchingKeys);
        result = 31 * result + Boolean.hashCode(sanityCheckJoins);
        return result;
    }

    @Override
    public String toString() {
        return "PartitionedTable.Proxy for " + target.table().getDescription();
    }

    @Override
    public PartitionedTable target() {
        return target;
    }

    private PartitionedTable.Proxy basicTransform(@NotNull final Function<Table, Table> transformer) {
        return new PartitionedTableProxyImpl(
                target.transform(transformer),
                requireMatchingKeys,
                sanityCheckJoins);
    }

    private PartitionedTable.Proxy complexTransform(
            @NotNull final TableOperations other,
            @NotNull final BinaryOperator<Table> transformer,
            @Nullable final Collection<? extends JoinMatch> joinMatches) {
        if (other instanceof Table) {
            final Table otherTable = (Table) other;
            return basicTransform(ct -> transformer.apply(ct, otherTable));
        }
        if (other instanceof PartitionedTable.Proxy) {
            final PartitionedTable otherPartitionedTable = ((PartitionedTable.Proxy) other).target();
            return new PartitionedTableProxyImpl(
                    target.partitionedTransform(otherPartitionedTable, transformer),
                    requireMatchingKeys,
                    sanityCheckJoins);
        }
        throw new IllegalArgumentException("Unexpected TableOperations input " + other
                + ", expected Table or PartitionedTable.Proxy");
    }

    private static List<JoinMatch> splitToJoinMatches(@NotNull final String columnsToMatch) {
        if (columnsToMatch.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(columnsToMatch.split(",")).map(String::trim).filter(ctm -> !ctm.isEmpty())
                .map(JoinMatch::parse).collect(Collectors.toList());
    }

    private static List<JoinMatch> splitToExactMatches(@NotNull final String columnsToMatch) {
        if (columnsToMatch.trim().isEmpty()) {
            return Collections.emptyList();
        }
        final String[] split = Arrays.stream(columnsToMatch.split(",")).map(String::trim).filter(ctm -> !ctm.isEmpty())
                .toArray(String[]::new);
        return Arrays.stream(split).limit(split.length - 1).map(JoinMatch::parse).collect(Collectors.toList());
    }

    /**
     * Get a table of keys that are uniquely in only {@code lhs} or {@code rhs}, with an additional column identifying
     * the table where the key was encountered.
     *
     * @param lhs The left-hand-side (first) partitioned table
     * @param rhs The right-hand-side (second) partitioned table
     * @return A table of keys that are uniquely in only one of the input partitioned tables
     */
    private static Table uniqueKeysTable(
            @NotNull final PartitionedTable lhs,
            @NotNull final PartitionedTable rhs) {
        final Table lhsKeys = lhs.table()
                .selectDistinct(lhs.keyColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        final Table rhsKeys = rhs.table()
                .selectDistinct(rhs.keyColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        final Table unionedKeys = TableTools.merge(lhsKeys, rhsKeys);
        final Table countedKeys = unionedKeys.countBy(FOUND_IN.name(), lhs.keyColumnNames());
        final Table uniqueKeys = countedKeys.where(FOUND_IN.name() + " != 2");
        return uniqueKeys.dropColumns(FOUND_IN.name());
    }

    private static class MatchingKeysEnforcementListener extends InstrumentedTableUpdateListenerAdapter {
        private final Table uniqueKeys;

        public MatchingKeysEnforcementListener(
                @NotNull final PartitionedTable lhs,
                @NotNull final PartitionedTable rhs,
                @NotNull final Table uniqueKeys) {
            super("Matching keys enforcement listener for " + lhs.table().getDescription()
                    + ", and " + rhs.table().getDescription(), uniqueKeys, false);
            this.uniqueKeys = uniqueKeys;
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            if (!uniqueKeys.isEmpty()) {
                throw formatUniqueKeysException(uniqueKeys);
            }
        }
    }

    private static IllegalArgumentException formatUniqueKeysException(@NotNull final Table uniqueKeys) {
        return new IllegalArgumentException("Partitioned table arguments have unique keys:\n"
                + tableToString(uniqueKeys, 10));
    }

    /**
     * Get a table of join keys that are found in more than one constituent table in {@code input}.
     *
     * @param input The input partitioned table
     * @param joinKeyColumnNames The exact match key column names for the join operation
     * @return A table of join keys that are found in more than one constituent table in {@code input}
     */
    private static Table overlappingJoinKeysTable(
            @NotNull final PartitionedTable input,
            @NotNull final String[] joinKeyColumnNames) {
        // NB: At the moment, we are assuming that constituents appear only once per partitioned table in scenarios
        // where overlapping join keys are concerning.
        final AtomicLong sequenceCounter = new AtomicLong(0);
        final PartitionedTable stamped = input.transform(table -> table
                .updateView(new LongConstantColumn(ENCLOSING_CONSTITUENT.name(), sequenceCounter.getAndIncrement())));
        final Table merged = stamped.merge();
        final Table mergedWithUniqueAgg = merged.aggAllBy(AggSpec.unique(), joinKeyColumnNames);
        final Table overlappingJoinKeys = mergedWithUniqueAgg.where(Filter.isNull(ENCLOSING_CONSTITUENT));
        return overlappingJoinKeys.view(joinKeyColumnNames);
    }

    private static class JoinSanityEnforcementListener extends InstrumentedTableUpdateListenerAdapter {

        private final String inputTableDescription;
        private final Table overlappingJoinKeys;

        public JoinSanityEnforcementListener(
                @NotNull final PartitionedTable input,
                @NotNull final Table overlappingJoinKeys) {
            super("Join sanity enforcement listener for " + input.table().getDescription(), overlappingJoinKeys, false);
            inputTableDescription = input.table().getDescription();
            this.overlappingJoinKeys = overlappingJoinKeys;
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            if (!overlappingJoinKeys.isEmpty()) {
                throw formatOverlappingJoinKeysException(inputTableDescription, overlappingJoinKeys);
            }
        }
    }

    private static IllegalArgumentException formatOverlappingJoinKeysException(
            @NotNull final String inputDescription,
            @NotNull final Table overlappingKeys) {
        return new IllegalArgumentException("Partitioned table \"" + inputDescription
                + "\" has join keys found in multiple constituents:\n"
                + tableToString(overlappingKeys, 10));
    }

    private static String tableToString(@NotNull final Table table, final int maximumRows) {
        try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                final PrintStream printStream = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            TableTools.show(table, maximumRows, TimeZone.TZ_DEFAULT, printStream);
            printStream.flush();
            return bytes.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // region TableOperations Implementation

    @Override
    public PartitionedTable.Proxy head(long size) {
        return basicTransform(ct -> ct.head(size));
    }

    @Override
    public PartitionedTable.Proxy tail(long size) {
        return basicTransform(ct -> ct.tail(size));
    }

    @Override
    public PartitionedTable.Proxy reverse() {
        return basicTransform(Table::reverse);
    }

    @Override
    public PartitionedTable.Proxy snapshot(TableOperations baseTable, String... stampColumns) {
        return complexTransform(baseTable, (ct, ot) -> ct.snapshot(ot, stampColumns), null);
    }

    @Override
    public PartitionedTable.Proxy snapshot(TableOperations baseTable, boolean doInitialSnapshot,
            String... stampColumns) {
        return complexTransform(baseTable, (ct, ot) -> ct.snapshot(ot, doInitialSnapshot, stampColumns), null);
    }

    @Override
    public PartitionedTable.Proxy snapshot(TableOperations baseTable, boolean doInitialSnapshot,
            Collection<ColumnName> stampColumns) {
        return complexTransform(baseTable, (ct, ot) -> ct.snapshot(ot, doInitialSnapshot, stampColumns), null);
    }

    @Override
    public PartitionedTable.Proxy sort(String... columnsToSortBy) {
        return basicTransform(ct -> ct.sort(columnsToSortBy));
    }

    @Override
    public PartitionedTable.Proxy sortDescending(String... columnsToSortBy) {
        return basicTransform(ct -> ct.sortDescending(columnsToSortBy));
    }

    @Override
    public PartitionedTable.Proxy sort(Collection<SortColumn> columnsToSortBy) {
        return basicTransform(ct -> ct.sort(columnsToSortBy));
    }

    @Override
    public PartitionedTable.Proxy where(String... filters) {
        return basicTransform(ct -> ct.where(filters));
    }

    @Override
    public PartitionedTable.Proxy where(Collection<? extends Filter> filters) {
        return basicTransform(ct -> ct.where(filters));
    }

    @Override
    public PartitionedTable.Proxy whereIn(TableOperations rightTable, String... columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.whereIn(ot, columnsToMatch), JoinMatch.from(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy whereIn(TableOperations rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.whereIn(ot, columnsToMatch), columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy whereNotIn(TableOperations rightTable, String... columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.whereNotIn(ot, columnsToMatch),
                JoinMatch.from(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy whereNotIn(TableOperations rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.whereNotIn(ot, columnsToMatch), columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy view(String... columns) {
        return basicTransform(ct -> ct.view(columns));
    }

    @Override
    public PartitionedTable.Proxy view(Collection<? extends Selectable> columns) {
        return basicTransform(ct -> ct.view(columns));
    }

    @Override
    public PartitionedTable.Proxy updateView(String... columns) {
        return basicTransform(ct -> ct.updateView(columns));
    }

    @Override
    public PartitionedTable.Proxy updateView(Collection<? extends Selectable> columns) {
        return basicTransform(ct -> ct.updateView(columns));
    }

    @Override
    public PartitionedTable.Proxy update(String... columns) {
        return basicTransform(ct -> ct.update(columns));
    }

    @Override
    public PartitionedTable.Proxy update(Collection<? extends Selectable> columns) {
        return basicTransform(ct -> ct.update(columns));
    }

    @Override
    public PartitionedTable.Proxy select(String... columns) {
        return basicTransform(ct -> ct.select(columns));
    }

    @Override
    public PartitionedTable.Proxy select(Collection<? extends Selectable> columns) {
        return basicTransform(ct -> ct.select(columns));
    }

    @Override
    public PartitionedTable.Proxy naturalJoin(TableOperations rightTable, String columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.naturalJoin(ot, columnsToMatch),
                splitToJoinMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy naturalJoin(TableOperations rightTable, String columnsToMatch, String columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.naturalJoin(ot, columnsToMatch, columnsToAdd),
                splitToJoinMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy naturalJoin(TableOperations rightTable,
            Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.naturalJoin(ot, columnsToMatch, columnsToAdd),
                columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy exactJoin(TableOperations rightTable, String columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.exactJoin(ot, columnsToMatch),
                splitToJoinMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy exactJoin(TableOperations rightTable, String columnsToMatch, String columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.exactJoin(ot, columnsToMatch, columnsToAdd),
                splitToJoinMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy exactJoin(TableOperations rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.exactJoin(ot, columnsToMatch, columnsToAdd), columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy join(TableOperations rightTable, String columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.join(ot, columnsToMatch),
                splitToJoinMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy join(TableOperations rightTable, String columnsToMatch, String columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.join(ot, columnsToMatch, columnsToAdd),
                splitToJoinMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy join(TableOperations rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.join(ot, columnsToMatch, columnsToAdd), columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy join(TableOperations rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int reserveBits) {
        return complexTransform(rightTable, (ct, ot) -> ct.join(ot, columnsToMatch, columnsToAdd, reserveBits),
                columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy aj(TableOperations rightTable, String columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.aj(ot, columnsToMatch), splitToExactMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy aj(TableOperations rightTable, String columnsToMatch, String columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.aj(ot, columnsToMatch, columnsToAdd),
                splitToExactMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy aj(TableOperations rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.aj(ot, columnsToMatch, columnsToAdd), columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy aj(TableOperations rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return complexTransform(rightTable, (ct, ot) -> ct.aj(ot, columnsToMatch, columnsToAdd, asOfJoinRule),
                columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy raj(TableOperations rightTable, String columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.raj(ot, columnsToMatch),
                splitToExactMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy raj(TableOperations rightTable, String columnsToMatch, String columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.raj(ot, columnsToMatch, columnsToAdd),
                splitToExactMatches(columnsToMatch));
    }

    @Override
    public PartitionedTable.Proxy raj(TableOperations rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.raj(ot, columnsToMatch, columnsToAdd), columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy raj(TableOperations rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return complexTransform(rightTable, (ct, ot) -> ct.raj(ot, columnsToMatch, columnsToAdd, reverseAsOfJoinRule),
                columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy groupBy() {
        return basicTransform(Table::groupBy);
    }

    @Override
    public PartitionedTable.Proxy groupBy(String... groupByColumns) {
        return basicTransform(ct -> ct.groupBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy groupBy(Collection<? extends Selectable> groupByColumns) {
        return basicTransform(ct -> ct.groupBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy aggAllBy(AggSpec spec) {
        return basicTransform(ct -> ct.aggAllBy(spec));
    }

    @Override
    public PartitionedTable.Proxy aggAllBy(AggSpec spec, String... groupByColumns) {
        return basicTransform(ct -> ct.aggAllBy(spec, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy aggAllBy(AggSpec spec, Selectable... groupByColumns) {
        return basicTransform(ct -> ct.aggAllBy(spec, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy aggAllBy(AggSpec spec, Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.aggAllBy(spec, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy aggBy(Aggregation aggregation) {
        return basicTransform(ct -> ct.aggBy(aggregation));
    }

    @Override
    public PartitionedTable.Proxy aggBy(Aggregation aggregation, String... groupByColumns) {
        return basicTransform(ct -> ct.aggBy(aggregation, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy aggBy(Aggregation aggregation, Collection<? extends Selectable> groupByColumns) {
        return basicTransform(ct -> ct.aggBy(aggregation, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy aggBy(Collection<? extends Aggregation> aggregations) {
        return basicTransform(ct -> ct.aggBy(aggregations));
    }

    @Override
    public PartitionedTable.Proxy aggBy(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        return basicTransform(ct -> ct.aggBy(aggregations, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy aggBy(Collection<? extends Aggregation> aggregations,
            Collection<? extends Selectable> groupByColumns) {
        return basicTransform(ct -> ct.aggBy(aggregations, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy selectDistinct() {
        return basicTransform(Table::selectDistinct);
    }

    @Override
    public PartitionedTable.Proxy selectDistinct(String... groupByColumns) {
        return basicTransform(ct -> ct.selectDistinct(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy selectDistinct(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.selectDistinct(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy selectDistinct(Collection<? extends Selectable> groupByColumns) {
        return basicTransform(ct -> ct.selectDistinct(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy countBy(String countColumnName) {
        return basicTransform(ct -> ct.countBy(countColumnName));
    }

    @Override
    public PartitionedTable.Proxy countBy(String countColumnName, String... groupByColumns) {
        return basicTransform(ct -> ct.countBy(countColumnName, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy countBy(String countColumnName, Selectable... groupByColumns) {
        return basicTransform(ct -> ct.countBy(countColumnName, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy countBy(String countColumnName, Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.countBy(countColumnName, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy firstBy() {
        return basicTransform(Table::firstBy);
    }

    @Override
    public PartitionedTable.Proxy firstBy(String... groupByColumns) {
        return basicTransform(ct -> ct.firstBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy firstBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.firstBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy firstBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.firstBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy lastBy() {
        return basicTransform(Table::lastBy);
    }

    @Override
    public PartitionedTable.Proxy lastBy(String... groupByColumns) {
        return basicTransform(ct -> ct.lastBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy lastBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.lastBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy lastBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.lastBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy minBy() {
        return basicTransform(Table::minBy);
    }

    @Override
    public PartitionedTable.Proxy minBy(String... groupByColumns) {
        return basicTransform(ct -> ct.minBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy minBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.minBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy minBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.minBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy maxBy() {
        return basicTransform(Table::maxBy);
    }

    @Override
    public PartitionedTable.Proxy maxBy(String... groupByColumns) {
        return basicTransform(ct -> ct.maxBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy maxBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.maxBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy maxBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.maxBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy sumBy() {
        return basicTransform(Table::sumBy);
    }

    @Override
    public PartitionedTable.Proxy sumBy(String... groupByColumns) {
        return basicTransform(ct -> ct.sumBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy sumBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.sumBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy sumBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.sumBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy avgBy() {
        return basicTransform(Table::avgBy);
    }

    @Override
    public PartitionedTable.Proxy avgBy(String... groupByColumns) {
        return basicTransform(ct -> ct.avgBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy avgBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.avgBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy avgBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.avgBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy medianBy() {
        return basicTransform(Table::medianBy);
    }

    @Override
    public PartitionedTable.Proxy medianBy(String... groupByColumns) {
        return basicTransform(ct -> ct.medianBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy medianBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.medianBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy medianBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.medianBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy stdBy() {
        return basicTransform(Table::stdBy);
    }

    @Override
    public PartitionedTable.Proxy stdBy(String... groupByColumns) {
        return basicTransform(ct -> ct.stdBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy stdBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.stdBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy stdBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.stdBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy varBy() {
        return basicTransform(Table::varBy);
    }

    @Override
    public PartitionedTable.Proxy varBy(String... groupByColumns) {
        return basicTransform(ct -> ct.varBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy varBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.varBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy varBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.varBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy absSumBy() {
        return basicTransform(Table::absSumBy);
    }

    @Override
    public PartitionedTable.Proxy absSumBy(String... groupByColumns) {
        return basicTransform(ct -> ct.absSumBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy absSumBy(Selectable... groupByColumns) {
        return basicTransform(ct -> ct.absSumBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy absSumBy(Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.absSumBy(groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy wsumBy(String weightColumn) {
        return basicTransform(ct -> ct.wsumBy(weightColumn));
    }

    @Override
    public PartitionedTable.Proxy wsumBy(String weightColumn, String... groupByColumns) {
        return basicTransform(ct -> ct.wsumBy(weightColumn, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy wsumBy(String weightColumn, Selectable... groupByColumns) {
        return basicTransform(ct -> ct.wsumBy(weightColumn, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy wsumBy(String weightColumn, Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.wsumBy(weightColumn, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy wavgBy(String weightColumn) {
        return basicTransform(ct -> ct.wavgBy(weightColumn));
    }

    @Override
    public PartitionedTable.Proxy wavgBy(String weightColumn, String... groupByColumns) {
        return basicTransform(ct -> ct.wavgBy(weightColumn, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy wavgBy(String weightColumn, Selectable... groupByColumns) {
        return basicTransform(ct -> ct.wavgBy(weightColumn, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy wavgBy(String weightColumn, Collection<String> groupByColumns) {
        return basicTransform(ct -> ct.wavgBy(weightColumn, groupByColumns));
    }

    // endregion TableOperations Implementation
}
