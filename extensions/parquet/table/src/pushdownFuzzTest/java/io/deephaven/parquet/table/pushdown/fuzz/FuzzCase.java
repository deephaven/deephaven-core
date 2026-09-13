//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * One fuzz case: the columns, the storage layout, the renames, the toggle profile and the filters.
 *
 * <p>
 * Everything is drawn from a single {@code Random(seed)}, so a case is fully reproducible from its seed alone. The
 * {@link #describe()} rendering is what goes into every assertion message, so a failure carries the complete
 * configuration rather than just a diff.
 *
 * <p>
 * Generation order matters and is fixed: columns and values first, then the layout (which marks the partitioning and
 * sort columns), then the renames (which set result names), then the filters (which must use result names).
 */
public final class FuzzCase {

    /** Row counts the size axis draws from. Zero and one carry real weight, on purpose. */
    private static final int[] SIZE_CHOICES = {0, 1, 2, 3, 7, 50, 200, 1_000, 10_000, 100_000};

    private final long seed;
    private final int tableSize;
    private final List<FuzzColumn> columns;
    private final FuzzLayout layout;
    private final FuzzRenames renames;
    private final FuzzToggles toggles;
    private final FuzzScope scope;
    private final List<FuzzFilters.FuzzFilter> filters;
    private final FuzzColumn sortColumn;
    private final FuzzColumn partitionColumn;

    private FuzzCase(
            final long seed,
            final int tableSize,
            final List<FuzzColumn> columns,
            final FuzzLayout layout,
            final FuzzRenames renames,
            final FuzzToggles toggles,
            final FuzzScope scope,
            final List<FuzzFilters.FuzzFilter> filters,
            final FuzzColumn sortColumn,
            final FuzzColumn partitionColumn) {
        this.seed = seed;
        this.tableSize = tableSize;
        this.columns = columns;
        this.layout = layout;
        this.renames = renames;
        this.toggles = toggles;
        this.scope = scope;
        this.filters = filters;
        this.sortColumn = sortColumn;
        this.partitionColumn = partitionColumn;
    }

    /** Build a case from its seed. Pure -- the same seed always yields the same case. */
    public static FuzzCase generate(final long seed, final FuzzConfig config) {
        final Random random = new Random(seed);

        // --- size -------------------------------------------------------------------------------
        final int tableSize;
        if (random.nextDouble() < config.emptyWeight) {
            tableSize = 0;
        } else {
            final List<Integer> allowed = new ArrayList<>();
            for (final int choice : SIZE_CHOICES) {
                if (choice <= config.maxTableSize) {
                    allowed.add(choice);
                }
            }
            tableSize = allowed.isEmpty() ? 0 : allowed.get(random.nextInt(allowed.size()));
        }

        // --- columns ----------------------------------------------------------------------------
        final int columnCount = 1 + random.nextInt(Math.max(1, config.maxColumns));
        final List<FuzzColumn> columns = new ArrayList<>(columnCount);
        for (int ii = 0; ii < columnCount; ++ii) {
            columns.add(buildColumn(random, "Col" + ii, tableSize));
        }

        // --- layout (marks the partitioning and sort columns) -----------------------------------
        final FuzzLayout layout = FuzzLayout.generate(
                random, columns, config.partitionedWeight, config.sortedWeight);

        // Data indexes on 0-2 columns, named in storage space by addIndexColumns. Chosen after the
        // layout, because a partitioning column already has an automatic PartitioningColumnDataIndex
        // and the writer rejects an explicit index on one ("Cannot add index on partitioning column").
        final int indexCount = random.nextInt(3);
        final List<FuzzColumn> indexable = new ArrayList<>();
        for (final FuzzColumn column : columns) {
            if (!column.partitioning()) {
                indexable.add(column);
            }
        }
        Collections.shuffle(indexable, random);
        for (int ii = 0; ii < Math.min(indexCount, indexable.size()); ++ii) {
            indexable.get(ii).setIndexed(true);
        }

        FuzzColumn sortColumn = null;
        FuzzColumn partitionColumn = null;
        for (final FuzzColumn column : columns) {
            if (column.sorted()) {
                sortColumn = column;
            }
            if (column.partitioning()) {
                partitionColumn = column;
            }
        }

        // --- renames ----------------------------------------------------------------------------
        // The partitioning column is excluded: its name comes from the directory key rather than
        // from the parquet schema, so addColumnNameMapping does not apply to it.
        final List<FuzzColumn> renameable = new ArrayList<>();
        for (final FuzzColumn column : columns) {
            if (!column.partitioning()) {
                renameable.add(column);
            }
        }
        final FuzzRenames renames = FuzzRenames.generate(random, renameable, config.renameWeight);

        // --- filters (result-space names) -------------------------------------------------------
        final FuzzScope scope = new FuzzScope();
        final List<FuzzColumn> filterable = new ArrayList<>(columns);
        // Duplicated columns are extra result-space names over an existing ColumnSource; make them
        // filterable so the identity-map hazard in computeRenameMap is actually reached.
        renames.duplicates().forEach((alias, sourceResultName) -> {
            for (final FuzzColumn column : columns) {
                if (column.resultName().equals(sourceResultName)) {
                    filterable.add(aliasOf(column, alias));
                    break;
                }
            }
        });
        if (layout.postRead() == FuzzLayout.PostRead.CONSTANT_COLUMN) {
            filterable.add(constantColumn(tableSize));
        }

        final List<FuzzFilters.FuzzFilter> filters = new ArrayList<>();
        for (int ii = 0; ii < config.filtersPerCase; ++ii) {
            filters.add(random.nextInt(4) == 0
                    ? FuzzFilters.composite(random, filterable, scope)
                    : FuzzFilters.generate(random, filterable, scope));
        }

        final FuzzToggles toggles = FuzzToggles.generate(random);

        return new FuzzCase(seed, tableSize, columns, layout, renames, toggles, scope, filters,
                sortColumn, partitionColumn);
    }

    /**
     * Build one column's values.
     *
     * <p>
     * Dictionary-friendly (String) columns are sometimes generated with cardinality that <em>varies along the row
     * axis</em>, in segments. That is what makes {@link FuzzLayout.DictionaryMode#MIXED} achievable: Deephaven's writer
     * decides dictionary encoding per row group, so only a column whose per-row-group distinct counts differ can
     * produce both encodings inside one file.
     */
    private static FuzzColumn buildColumn(final Random random, final String name, final int tableSize) {
        final FuzzType type = FuzzType.random(random);

        final FuzzType.NullMode[] nullModes = {
                FuzzType.NullMode.NONE, FuzzType.NullMode.NONE,
                FuzzType.NullMode.SPARSE, FuzzType.NullMode.HEAVY, FuzzType.NullMode.ALL};
        final FuzzType.NullMode nullMode = nullModes[random.nextInt(nullModes.length)];

        final FuzzType.Spread[] spreads = {
                FuzzType.Spread.NARROW, FuzzType.Spread.WIDE,
                FuzzType.Spread.EXTREMES, FuzzType.Spread.EXTREMES};
        final FuzzType.Spread spread = spreads[random.nextInt(spreads.length)];

        final List<Object> values = new ArrayList<>(tableSize);
        final boolean segmentedCardinality = type.dictionaryFriendly() && tableSize > 8 && random.nextBoolean();
        if (segmentedCardinality) {
            // Four segments of increasing cardinality: 2, 6, 22, 86 distinct values.
            final int segments = 4;
            final int perSegment = Math.max(1, tableSize / segments);
            for (int ii = 0; ii < tableSize; ++ii) {
                final int segment = Math.min(segments - 1, ii / perSegment);
                final int pool = (int) (2 * Math.pow(4, segment)) + segment * 2;
                if (nullMode == FuzzType.NullMode.ALL
                        || (nullMode.fraction > 0 && random.nextDouble() < nullMode.fraction)) {
                    values.add(null);
                } else {
                    values.add("seg" + segment + "_" + random.nextInt(pool));
                }
            }
        } else {
            for (int ii = 0; ii < tableSize; ++ii) {
                values.add(type.draw(random, spread, nullMode));
            }
        }
        return new FuzzColumn(type, name, nullMode, spread, values);
    }

    /** A stand-in {@link FuzzColumn} for a duplicated (aliased) result column. */
    private static FuzzColumn aliasOf(final FuzzColumn source, final String alias) {
        final FuzzColumn column = new FuzzColumn(
                source.type(), alias, source.nullMode(), source.spread(), new ArrayList<>(source.values()));
        column.setResultName(alias);
        return column;
    }

    /** A stand-in {@link FuzzColumn} for the constant column added by the CONSTANT_COLUMN post-read. */
    private static FuzzColumn constantColumn(final int tableSize) {
        final List<Object> values = new ArrayList<>(tableSize);
        for (int ii = 0; ii < tableSize; ++ii) {
            values.add(42);
        }
        final FuzzColumn column = new FuzzColumn(
                FuzzType.INT, "FuzzConst", FuzzType.NullMode.NONE, FuzzType.Spread.NARROW, values);
        column.setResultName("FuzzConst");
        return column;
    }

    /** Materialize the source table, in storage-name space. */
    public Table buildSourceTable() {
        final ColumnHolder<?>[] holders = new ColumnHolder<?>[columns.size()];
        for (int ii = 0; ii < holders.length; ++ii) {
            holders[ii] = columns.get(ii).toColumnHolder();
        }
        return TableTools.newTable(holders);
    }

    public long seed() {
        return seed;
    }

    public int tableSize() {
        return tableSize;
    }

    public List<FuzzColumn> columns() {
        return Collections.unmodifiableList(columns);
    }

    public FuzzLayout layout() {
        return layout;
    }

    public FuzzRenames renames() {
        return renames;
    }

    public FuzzToggles toggles() {
        return toggles;
    }

    public FuzzScope scope() {
        return scope;
    }

    public List<FuzzFilters.FuzzFilter> filters() {
        return Collections.unmodifiableList(filters);
    }

    public FuzzColumn sortColumn() {
        return sortColumn;
    }

    public FuzzColumn partitionColumn() {
        return partitionColumn;
    }

    /** A pasteable description of the whole case, used in every assertion message. */
    public String describe() {
        final StringBuilder sb = new StringBuilder();
        sb.append("\n// Seed: ").append(seed).append("L\n");
        sb.append("rows=").append(tableSize).append('\n');
        for (final FuzzColumn column : columns) {
            sb.append("  column ").append(column).append('\n');
        }
        sb.append("  ").append(layout).append('\n');
        sb.append("  ").append(renames).append('\n');
        sb.append("  ").append(toggles).append('\n');
        if (!scope.bound().isEmpty()) {
            sb.append("  params=").append(scope).append('\n');
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "FuzzCase{seed=" + seed + "L, rows=" + tableSize + ", columns=" + columns.size() + '}';
    }

    /** The size choices, exposed for the coverage report. */
    public static int[] sizeChoices() {
        return Arrays.copyOf(SIZE_CHOICES, SIZE_CHOICES.length);
    }
}
