//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.metadata.RowGroupInfo;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * The storage-and-execution layout for one fuzz case: how the data reaches disk, and what shape the table has by the
 * time a filter runs against it.
 *
 * <p>
 * Layout is a first-class dimension because it decides <em>which</em> pushdown mechanism can serve a filter. A
 * single-file sorted table takes the table-level {@code SortedColumnPushdownManager} path, which discards the regioned
 * matcher entirely; a multi-file or partitioned sorted table takes the per-region sorted actions instead, because
 * {@code SourceTable} publishes the sorted attribute only when there is exactly one included location. Same data, same
 * declared order, two disjoint code paths.
 */
public final class FuzzLayout {

    /** How the data is spread across files. */
    public enum FileLayout {
        /** One file, one location. */
        SINGLE_FILE,
        /** Several flat files in a directory, read as one table. */
        FLAT_MULTI,
        /** Key-value partitioned: the only layout that produces a {@code PartitionAwareSourceTable}. */
        PARTITIONED
    }

    /**
     * Where sortedness is declared, which selects the pushdown path.
     *
     * <p>
     * Every mode that declares an order first performs the corresponding engine sort, because
     * {@code SortedColumnsAttribute.withOrderForColumn} performs <em>no validation</em>: declaring an order the data
     * does not actually have makes range and match filters silently return wrong rows. A fuzzer that mis-declared would
     * manufacture false failures, so the declaration here is always truthful by construction.
     */
    public enum SortMode {
        /** No sorting at all. */
        NONE,
        /** Sorted data, order recorded in file metadata, single file -- reaches the table-level manager. */
        SINGLE_FILE_METADATA,
        /** Each file sorted and declared independently -- reaches region sorted actions only. */
        PER_SLICE_METADATA,
        /** Globally sorted, attribute applied after the read -- table-level only, no file metadata. */
        ATTRIBUTE_AFTER_READ,
        /** Sorted data with nothing declared -- the control; statistics/dictionary only. */
        DATA_ONLY,
        /** File metadata plus a re-applied attribute. */
        BOTH
    }

    /** Dictionary-encoding strategy for String columns (the only ones Deephaven dictionary-encodes). */
    public enum DictionaryMode {
        /** Writer defaults. */
        DEFAULT,
        /** {@code useDictionary(col, true)} -- always encode, regardless of limits. */
        FORCE_ON,
        /** A zero key limit, so the writer immediately falls back to PLAIN. */
        FORCE_OFF,
        /**
         * A key limit chosen between the per-row-group distinct counts, so some row groups are dictionary-encoded and
         * others fall back to PLAIN within one file.
         */
        MIXED
    }

    /** Shape changes applied between the read and the filter. */
    public enum PostRead {
        NONE,
        /** {@code merge} of two independent reads -- union pushdown. */
        MERGE,
        /** An added constant column -- single-value/constant-region pushdown. */
        CONSTANT_COLUMN,
        /**
         * An explicit {@code coalesce()} before the filter. On a partitioned table this bypasses the
         * partitioning/pushdown filter split, so partitioning-column filters go through pushdown.
         */
        COALESCE_FIRST
    }

    /** Restriction of the row set the filter sees, so pushdown gets a partial selection. */
    public enum Selection {
        ALL, HEAD, TAIL, SLICE
    }

    private final FileLayout fileLayout;
    private final int fileCount;
    private final boolean randomSplitSizes;
    private final SortMode sortMode;
    private final SortingOrder sortOrder;
    private final DictionaryMode dictionaryMode;
    private final boolean writeStatistics;
    private final Integer targetPageSize;
    private final Long rowGroupMaxRows;
    private final Integer rowGroupMaxGroups;
    private final boolean generateMetadataFiles;
    private final PostRead postRead;
    private final Selection selection;

    private FuzzLayout(
            final FileLayout fileLayout,
            final int fileCount,
            final boolean randomSplitSizes,
            final SortMode sortMode,
            final SortingOrder sortOrder,
            final DictionaryMode dictionaryMode,
            final boolean writeStatistics,
            final Integer targetPageSize,
            final Long rowGroupMaxRows,
            final Integer rowGroupMaxGroups,
            final boolean generateMetadataFiles,
            final PostRead postRead,
            final Selection selection) {
        this.fileLayout = fileLayout;
        this.fileCount = fileCount;
        this.randomSplitSizes = randomSplitSizes;
        this.sortMode = sortMode;
        this.sortOrder = sortOrder;
        this.dictionaryMode = dictionaryMode;
        this.writeStatistics = writeStatistics;
        this.targetPageSize = targetPageSize;
        this.rowGroupMaxRows = rowGroupMaxRows;
        this.rowGroupMaxGroups = rowGroupMaxGroups;
        this.generateMetadataFiles = generateMetadataFiles;
        this.postRead = postRead;
        this.selection = selection;
    }

    /**
     * Draw a layout.
     *
     * @param random source of randomness
     * @param columns the case's columns; a sort column and a partitioning column may be marked here
     * @param partitionedProbability the chance of using a key-value partitioned layout
     * @param sortedProbability the chance of the case having a sorted column
     */
    public static FuzzLayout generate(
            final Random random,
            final List<FuzzColumn> columns,
            final double partitionedProbability,
            final double sortedProbability) {

        // --- file layout ------------------------------------------------------------------------
        final FileLayout fileLayout;
        final List<FuzzColumn> partitionable = new ArrayList<>();
        for (final FuzzColumn column : columns) {
            // A partitioning column must round-trip through a directory name, and must be low
            // cardinality or the case explodes into thousands of directories.
            if (column.type().partitionable() && column.spread() == FuzzType.Spread.NARROW
                    && column.nullMode() == FuzzType.NullMode.NONE) {
                partitionable.add(column);
            }
        }
        // Two API preconditions of a key-value partitioned write, neither of which hides a defect:
        // - it requires at least one non-partitioning column, so a single-column case can never be
        // partitioned ("Cannot write a partitioned parquet table without any non-partitioning columns");
        // - an empty table has no partitions, so the write produces an empty directory, and the key-value
        // layout records the schema only in its data files -- readTable then reports "Unable to infer
        // schema for a partitioned parquet table when there are no initial parquet files". Asserted in
        // EmptyPartitionedTableWriteTest. (The write used to crash with ArrayIndexOutOfBoundsException
        // instead; that was finding 4 and is fixed.)
        final boolean anyRows = columns.stream().anyMatch(c -> c.size() > 0);
        if (!partitionable.isEmpty() && columns.size() >= 2 && anyRows
                && random.nextDouble() < partitionedProbability) {
            fileLayout = FileLayout.PARTITIONED;
            partitionable.get(random.nextInt(partitionable.size())).setPartitioning(true);
        } else if (random.nextInt(3) == 0) {
            fileLayout = FileLayout.SINGLE_FILE;
        } else {
            fileLayout = FileLayout.FLAT_MULTI;
        }

        final int fileCount = fileLayout == FileLayout.FLAT_MULTI ? 2 + random.nextInt(11) : 1;
        final boolean randomSplitSizes = random.nextBoolean();

        // --- sortedness -------------------------------------------------------------------------
        final List<FuzzColumn> sortable = new ArrayList<>();
        for (final FuzzColumn column : columns) {
            if (column.type().sortable() && !column.partitioning()) {
                sortable.add(column);
            }
        }
        SortMode sortMode = SortMode.NONE;
        SortingOrder sortOrder = SortingOrder.Ascending;
        if (!sortable.isEmpty() && random.nextDouble() < sortedProbability) {
            sortable.get(random.nextInt(sortable.size())).setSorted(true);
            sortOrder = random.nextBoolean() ? SortingOrder.Ascending : SortingOrder.Descending;
            final List<SortMode> modes = new ArrayList<>(Arrays.asList(
                    SortMode.DATA_ONLY, SortMode.ATTRIBUTE_AFTER_READ, SortMode.BOTH));
            if (fileLayout == FileLayout.SINGLE_FILE) {
                modes.add(SortMode.SINGLE_FILE_METADATA);
                modes.add(SortMode.SINGLE_FILE_METADATA);
            } else {
                modes.add(SortMode.PER_SLICE_METADATA);
                modes.add(SortMode.PER_SLICE_METADATA);
            }
            sortMode = modes.get(random.nextInt(modes.size()));
        }

        // --- row groups, pages, statistics, dictionary -------------------------------------------
        final Long rowGroupMaxRows;
        final Integer rowGroupMaxGroups;
        final int rowGroupRoll = random.nextInt(4);
        if (rowGroupRoll == 0) {
            rowGroupMaxRows = null;
            rowGroupMaxGroups = null; // singleGroup
        } else if (rowGroupRoll == 1) {
            rowGroupMaxRows = null;
            rowGroupMaxGroups = 1 + random.nextInt(5);
        } else if (rowGroupRoll == 2) {
            // Deliberately tiny, to produce many row groups (and empty ones on short tables).
            final long[] choices = {1L, 2L, 3L, 7L, 13L, 64L, 257L};
            rowGroupMaxRows = choices[random.nextInt(choices.length)];
            rowGroupMaxGroups = null;
        } else {
            rowGroupMaxRows = null;
            rowGroupMaxGroups = null; // writer default
        }

        // MIN_TARGET_PAGE_SIZE is 2KB; shrinking pages keeps multi-page structure on small tables.
        final Integer targetPageSize = random.nextInt(3) == 0
                ? ParquetInstructions.MIN_TARGET_PAGE_SIZE
                : null;

        final boolean writeStatistics = random.nextInt(5) != 0;
        final boolean generateMetadataFiles = fileLayout != FileLayout.SINGLE_FILE && random.nextInt(6) == 0;

        boolean anyDictionaryFriendly = false;
        for (final FuzzColumn column : columns) {
            anyDictionaryFriendly |= column.type().dictionaryFriendly();
        }
        final DictionaryMode dictionaryMode;
        if (!anyDictionaryFriendly) {
            dictionaryMode = DictionaryMode.DEFAULT;
        } else {
            final DictionaryMode[] modes = {
                    DictionaryMode.DEFAULT, DictionaryMode.FORCE_ON,
                    DictionaryMode.FORCE_OFF, DictionaryMode.MIXED, DictionaryMode.MIXED};
            dictionaryMode = modes[random.nextInt(modes.length)];
        }

        // --- post-read shape and selection -------------------------------------------------------
        final PostRead[] postReads = {
                PostRead.NONE, PostRead.NONE, PostRead.NONE,
                PostRead.MERGE, PostRead.CONSTANT_COLUMN, PostRead.COALESCE_FIRST};
        final PostRead postRead = postReads[random.nextInt(postReads.length)];

        final Selection[] selections = {
                Selection.ALL, Selection.ALL, Selection.ALL,
                Selection.HEAD, Selection.TAIL, Selection.SLICE};
        final Selection selection = selections[random.nextInt(selections.length)];

        return new FuzzLayout(fileLayout, fileCount, randomSplitSizes, sortMode, sortOrder, dictionaryMode,
                writeStatistics, targetPageSize, rowGroupMaxRows, rowGroupMaxGroups, generateMetadataFiles,
                postRead, selection);
    }

    public FileLayout fileLayout() {
        return fileLayout;
    }

    public SortMode sortMode() {
        return sortMode;
    }

    public SortingOrder sortOrder() {
        return sortOrder;
    }

    public PostRead postRead() {
        return postRead;
    }

    public DictionaryMode dictionaryMode() {
        return dictionaryMode;
    }

    // -------------------------------------------------------------------------------------------
    // Write side
    // -------------------------------------------------------------------------------------------

    /**
     * Sort the source table if the case calls for it. Performed before writing so any declared order is truthful.
     *
     * @return the (possibly sorted) table, and whether the whole table is globally sorted
     */
    public Table sortForWrite(final Table source, final FuzzColumn sortColumn) {
        if (sortMode == SortMode.NONE || sortColumn == null) {
            return source;
        }
        if (sortMode == SortMode.PER_SLICE_METADATA && fileLayout == FileLayout.FLAT_MULTI) {
            // Flat slices are sorted individually in write(), so the table as a whole is not sorted
            // and only the per-region sorted actions can apply.
            return source;
        }
        // For a partitioned layout, sorting globally first is what makes a per-partition declaration
        // truthful: partitionBy preserves the source's relative row order within each constituent,
        // so every partition of a globally sorted table is itself sorted.
        return sortOrder == SortingOrder.Ascending
                ? source.sort(sortColumn.storageName())
                : source.sortDescending(sortColumn.storageName());
    }

    /** Build the write instructions for this layout. */
    /** Build the write instructions. */
    public ParquetInstructions writeInstructions(
            final List<FuzzColumn> columns,
            final int tableSize) {
        final ParquetInstructions.Builder builder = new ParquetInstructions.Builder();

        if (rowGroupMaxRows != null) {
            builder.setRowGroupInfo(RowGroupInfo.maxRows(rowGroupMaxRows));
        } else if (rowGroupMaxGroups != null) {
            builder.setRowGroupInfo(RowGroupInfo.maxGroups(rowGroupMaxGroups));
        }
        if (targetPageSize != null) {
            builder.setTargetPageSize(targetPageSize);
        }
        if (!writeStatistics) {
            builder.setWriteRowGroupStatistics(false);
        }
        if (generateMetadataFiles) {
            builder.setGenerateMetadataFiles(true);
        }

        applyDictionaryMode(builder, columns, tableSize);

        // Data indexes, named in storage space.
        final List<String> indexColumns = new ArrayList<>();
        for (final FuzzColumn column : columns) {
            if (column.indexed()) {
                indexColumns.add(column.storageName());
            }
        }
        if (!indexColumns.isEmpty()) {
            builder.addIndexColumns(indexColumns.toArray(new String[0]));
        }
        return builder.build();
    }

    /**
     * Configure dictionary encoding.
     *
     * <p>
     * Deephaven's writer attempts dictionary encoding per column chunk (per row group) for String columns only, and on
     * {@code DictionarySizeExceededException} discards the dictionary and re-encodes the <em>whole chunk</em> as PLAIN.
     * So to get both encodings inside one file, the key limit has to fall between the per-row-group distinct counts --
     * which is what {@link DictionaryMode#MIXED} computes from the actual data.
     */
    private void applyDictionaryMode(
            final ParquetInstructions.Builder builder,
            final List<FuzzColumn> columns,
            final int tableSize) {
        if (dictionaryMode == DictionaryMode.DEFAULT) {
            return;
        }
        for (final FuzzColumn column : columns) {
            if (!column.type().dictionaryFriendly()) {
                continue;
            }
            switch (dictionaryMode) {
                case FORCE_ON:
                    builder.useDictionary(column.storageName(), true);
                    break;
                case FORCE_OFF:
                    builder.setMaximumDictionaryKeys(0);
                    break;
                case MIXED: {
                    final int limit = mixedDictionaryKeyLimit(column, tableSize);
                    if (limit > 0) {
                        builder.setMaximumDictionaryKeys(limit);
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }

    /**
     * Pick a dictionary key limit strictly between the smallest and largest per-row-group distinct count, so the writer
     * encodes some row groups and falls back on others.
     *
     * @return the limit, or 0 if the data has uniform cardinality and mixing is not achievable
     */
    private int mixedDictionaryKeyLimit(final FuzzColumn column, final int tableSize) {
        final long window = rowGroupMaxRows != null
                ? rowGroupMaxRows
                : Math.max(1L, tableSize / Math.max(1, rowGroupMaxGroups == null ? 4 : rowGroupMaxGroups));
        final List<Object> values = column.values();
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (int start = 0; start < values.size(); start += window) {
            final int end = (int) Math.min(values.size(), start + window);
            final long distinct = values.subList(start, end).stream()
                    .filter(java.util.Objects::nonNull)
                    .distinct()
                    .count();
            min = Math.min(min, (int) distinct);
            max = Math.max(max, (int) distinct);
        }
        if (max <= min + 1) {
            return 0;
        }
        // Strictly between: row groups with <= limit distinct values encode, the rest fall back.
        return min + 1;
    }

    /**
     * Write {@code source} to {@code destPath} in this layout.
     *
     * @return the path to hand to {@code readTable}
     */
    public String write(
            final Table source,
            final String destPath,
            final ParquetInstructions instructions,
            final List<FuzzColumn> columns,
            final FuzzColumn sortColumn) {
        switch (fileLayout) {
            case SINGLE_FILE: {
                Table toWrite = source;
                if (declaresFileMetadata() && sortColumn != null) {
                    toWrite = SortedColumnsAttribute.withOrderForColumn(
                            toWrite, sortColumn.storageName(), sortOrder);
                }
                final String file = Path.of(destPath, "table.parquet").toString();
                ParquetTools.writeTable(toWrite, file, instructions);
                return file;
            }
            case FLAT_MULTI: {
                final Table[] slices = split(source, fileCount, randomSplitSizes);
                for (int ii = 0; ii < slices.length; ++ii) {
                    Table slice = slices[ii];
                    if (sortMode == SortMode.PER_SLICE_METADATA && sortColumn != null) {
                        // Sort this slice, then declare its order. The table as a whole is unsorted,
                        // so only the per-region sorted actions can apply.
                        slice = sortOrder == SortingOrder.Ascending
                                ? slice.sort(sortColumn.storageName())
                                : slice.sortDescending(sortColumn.storageName());
                        slice = SortedColumnsAttribute.withOrderForColumn(
                                slice, sortColumn.storageName(), sortOrder);
                    } else if (declaresFileMetadata() && sortColumn != null) {
                        slice = SortedColumnsAttribute.withOrderForColumn(
                                slice, sortColumn.storageName(), sortOrder);
                    }
                    final String name = String.format("table_%05d.parquet", ii);
                    ParquetTools.writeTable(slice, Path.of(destPath, name).toString(), instructions);
                }
                return destPath;
            }
            case PARTITIONED: {
                FuzzColumn partitionColumn = null;
                for (final FuzzColumn column : columns) {
                    if (column.partitioning()) {
                        partitionColumn = column;
                        break;
                    }
                }
                if (partitionColumn == null) {
                    throw new IllegalStateException("PARTITIONED layout without a partitioning column");
                }
                Table toWrite = source;
                if (declaresFileMetadata() && sortColumn != null) {
                    toWrite = SortedColumnsAttribute.withOrderForColumn(
                            toWrite, sortColumn.storageName(), sortOrder);
                }
                final PartitionedTable partitioned = toWrite.partitionBy(partitionColumn.storageName());
                ParquetTools.writeKeyValuePartitionedTable(partitioned, destPath, instructions);
                return destPath;
            }
            default:
                throw new IllegalStateException("Unhandled layout " + fileLayout);
        }
    }

    private boolean declaresFileMetadata() {
        return declaresFileMetadata(sortMode);
    }

    /** Whether {@code mode} records the sort order in parquet file metadata. */
    private static boolean declaresFileMetadata(final SortMode mode) {
        return mode == SortMode.SINGLE_FILE_METADATA || mode == SortMode.PER_SLICE_METADATA
                || mode == SortMode.BOTH;
    }

    /** Split into {@code count} slices, evenly or at random sizes. Some slices may be empty. */
    private static Table[] split(final Table source, final int count, final boolean randomSizes) {
        if (count <= 1) {
            return new Table[] {source};
        }
        final long size = source.size();
        final Table[] slices = new Table[count];
        if (!randomSizes) {
            final long each = size / count;
            for (int ii = 0; ii < count - 1; ++ii) {
                slices[ii] = source.slice(ii * each, (ii + 1) * each);
            }
            slices[count - 1] = source.slice((count - 1) * each, size);
            return slices;
        }
        // Random cut points, which naturally produces empty slices -- an empty file next to
        // populated ones is exactly the emptiness shape that reaches pushdown with empty regions.
        final long[] cuts = new long[count + 1];
        cuts[count] = size;
        final Random random = new Random(size * 31L + count);
        for (int ii = 1; ii < count; ++ii) {
            cuts[ii] = size == 0 ? 0 : Math.floorMod(random.nextLong(), size + 1);
        }
        Arrays.sort(cuts);
        for (int ii = 0; ii < count; ++ii) {
            slices[ii] = source.slice(cuts[ii], cuts[ii + 1]);
        }
        return slices;
    }

    // -------------------------------------------------------------------------------------------
    // Read side
    // -------------------------------------------------------------------------------------------

    /**
     * Apply the post-read shape changes, in the order the plan calls for: renames first (done by the caller), then
     * sortedness re-declaration, then the shape transform, then the selection restriction.
     */
    public Table applyAfterRead(Table table, final FuzzColumn sortColumn) {
        switch (postRead) {
            case MERGE:
                // Two reads of the same data merged: a union with two constituents.
                table = TableTools.merge(table, table);
                break;
            case CONSTANT_COLUMN:
                table = table.updateView("FuzzConst = 42");
                break;
            case COALESCE_FIRST:
                table = table.coalesce();
                break;
            case NONE:
            default:
                break;
        }
        // The attribute is declared AFTER the shape transform, and never on a merged table: a
        // concatenation of two sorted tables is not sorted, so declaring one here would be a lie by
        // the bench, and withOrderForColumn performs no validation. (The engine reaching the same
        // false claim on its own, by propagating the attribute through merge, is a separate and real
        // finding -- see MergeSortedAttributeFindingTest.)
        // Not for a partitioned layout either. sortForWrite sorts globally so that every *partition* is
        // sorted, which is what the per-file metadata claims and is true. But the read-back table is the
        // partitions concatenated in partition-key order, and that is not the sort order -- so a table-wide
        // declaration here would be a lie by the bench, and withOrderForColumn validates nothing. (Seed
        // -1220343102263136052 was exactly this: Col1 ascending within each Col0 partition, and the whole
        // table claimed ascending, so a range filter binary-searched garbage and returned 39 rows of 13.)
        if ((sortMode == SortMode.ATTRIBUTE_AFTER_READ || sortMode == SortMode.BOTH)
                && sortColumn != null
                && postRead != PostRead.MERGE
                && fileLayout != FileLayout.PARTITIONED) {
            // Declared on the result name: the attribute lives in result space, so the region
            // sorted action has to translate it back through the rename map.
            table = SortedColumnsAttribute.withOrderForColumn(table, sortColumn.resultName(), sortOrder);
        }
        return table;
    }

    /** Restrict the rows the filter sees, so pushdown receives a partial selection. */
    public Table applySelection(final Table table) {
        final long size = table.size();
        if (selection == Selection.ALL || size == 0) {
            return table;
        }
        switch (selection) {
            case HEAD:
                return table.head(Math.max(0L, size / 2));
            case TAIL:
                return table.tail(Math.max(0L, size / 2));
            case SLICE:
                return table.slice(size / 4, size - size / 4);
            default:
                return table;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("layout=").append(fileLayout);
        if (fileLayout == FileLayout.FLAT_MULTI) {
            sb.append('(').append(fileCount).append(randomSplitSizes ? ",random" : ",even").append(')');
        }
        if (rowGroupMaxRows != null) {
            sb.append(" rowGroups=maxRows(").append(rowGroupMaxRows).append(')');
        } else if (rowGroupMaxGroups != null) {
            sb.append(" rowGroups=maxGroups(").append(rowGroupMaxGroups).append(')');
        } else {
            sb.append(" rowGroups=default");
        }
        if (targetPageSize != null) {
            sb.append(" pageSize=").append(targetPageSize);
        }
        if (!writeStatistics) {
            sb.append(" noStatistics");
        }
        if (generateMetadataFiles) {
            sb.append(" metadataFiles");
        }
        sb.append(" dictionary=").append(dictionaryMode);
        if (sortMode != SortMode.NONE) {
            sb.append(" sort=").append(sortMode).append('/').append(sortOrder);
        }
        if (postRead != PostRead.NONE) {
            sb.append(" postRead=").append(postRead);
        }
        if (selection != Selection.ALL) {
            sb.append(" selection=").append(selection);
        }
        return sb.toString();
    }
}
