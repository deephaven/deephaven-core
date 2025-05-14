//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.layout.IcebergAutoRefreshTableLocationProvider;
import io.deephaven.iceberg.layout.IcebergBaseLayout;
import io.deephaven.iceberg.layout.IcebergManualRefreshTableLocationProvider;
import io.deephaven.iceberg.layout.IcebergStaticTableLocationProvider;
import io.deephaven.iceberg.layout.IcebergTableLocationProviderBase;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mapping.NameMapping;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * This class manages an Iceberg {@link org.apache.iceberg.Table table} and provides methods to interact with it.
 */
public final class IcebergTableAdapter {
    @VisibleForTesting
    static final TableDefinition SNAPSHOT_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("Id"),
            ColumnDefinition.ofTime("Timestamp"),
            ColumnDefinition.ofString("Operation"),
            ColumnDefinition.fromGenericType("Summary", Map.class),
            ColumnDefinition.fromGenericType("SnapshotObject", Snapshot.class));

    private static final Set<String> S3_SCHEMES = Set.of("s3", "s3a", "s3n");

    private final Catalog catalog;
    private final org.apache.iceberg.Table table;
    private final TableIdentifier tableIdentifier;
    private final DataInstructionsProviderLoader dataInstructionsProviderLoader;
    private final URI locationUri;
    private final Resolver resolver;
    private final NameMapping nameMapping;

    public IcebergTableAdapter(
            final Catalog catalog,
            final TableIdentifier tableIdentifier,
            final org.apache.iceberg.Table table,
            final DataInstructionsProviderLoader dataInstructionsProviderLoader,
            final Resolver resolver,
            final NameMapping nameMapping) {
        this.catalog = Objects.requireNonNull(catalog);
        this.table = Objects.requireNonNull(table);
        this.tableIdentifier = Objects.requireNonNull(tableIdentifier);
        this.dataInstructionsProviderLoader = Objects.requireNonNull(dataInstructionsProviderLoader);
        this.locationUri = IcebergUtils.locationUri(table);
        this.resolver = Objects.requireNonNull(resolver);
        this.nameMapping = Objects.requireNonNull(nameMapping);
    }

    /**
     * {@link Catalog} used to access this table.
     */
    public Catalog catalog() {
        return catalog;
    }

    /**
     * Get the Iceberg {@link TableIdentifier table identifier}.
     */
    public TableIdentifier tableIdentifier() {
        return tableIdentifier;
    }

    /**
     * The resolver.
     */
    public Resolver resolver() {
        return resolver;
    }

    /**
     * The name mapping.
     */
    public NameMapping nameMapping() {
        return nameMapping;
    }

    /**
     * Get the current {@link Snapshot snapshot} of a given Iceberg table or {@code null} if there are no snapshots.
     *
     * @return The current snapshot of the table or {@code null} if there are no snapshots.
     */
    public synchronized Snapshot currentSnapshot() {
        // Refresh the table to update the current snapshot.
        refresh();
        return table.currentSnapshot();
    }

    /**
     * Get the current list of all {@link Snapshot snapshots} of the Iceberg table.
     *
     * @return A list of all snapshots of the given table.
     */
    public synchronized List<Snapshot> listSnapshots() {
        // Refresh the table to update the snapshot list.
        refresh();
        return getSnapshots();
    }

    /**
     * Get a list of all {@link Snapshot snapshots} of the Iceberg table (without refreshing).
     *
     * @return A list of all snapshots of the given table.
     */
    private List<Snapshot> getSnapshots() {
        final List<Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        return snapshots;
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the following columns:
     * <table>
     * <caption></caption>
     * <tr>
     * <th>Column Name</th>
     * <th>Description</th>
     * </tr>
     * <tr>
     * <td>Id</td>
     * <td>The snapshot identifier (can be used for updating the table or loading a specific snapshot)</td>
     * </tr>
     * <tr>
     * <td>Timestamp</td>
     * <td>The timestamp of the snapshot</td>
     * </tr>
     * <tr>
     * <td>Operation</td>
     * <td>The data operation that created this snapshot</td>
     * </tr>
     * <tr>
     * <td>Summary</td>
     * <td>Additional information about the snapshot from the Iceberg metadata</td>
     * </tr>
     * <tr>
     * <td>SnapshotObject</td>
     * <td>A Java object containing the Iceberg API snapshot</td>
     * </tr>
     * <tr>
     * </tr>
     * </table>
     *
     * @return A Table containing a list of all tables in the given namespace.
     */
    public Table snapshots() {
        // Retrieve the current list of snapshots
        final List<Snapshot> snapshots = listSnapshots();
        final long size = snapshots.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final long[] idArr = new long[(int) size];
        columnSourceMap.put("Id", InMemoryColumnSource.getImmutableMemoryColumnSource(idArr, long.class, null));

        final long[] timestampArr = new long[(int) size];
        columnSourceMap.put("Timestamp",
                InMemoryColumnSource.getImmutableMemoryColumnSource(timestampArr, Instant.class, null));

        final String[] operatorArr = new String[(int) size];
        columnSourceMap.put("Operation",
                InMemoryColumnSource.getImmutableMemoryColumnSource(operatorArr, String.class, null));

        final Map<String, String>[] summaryArr = new Map[(int) size];
        columnSourceMap.put("Summary",
                InMemoryColumnSource.getImmutableMemoryColumnSource(summaryArr, Map.class, null));

        final Snapshot[] snapshotArr = new Snapshot[(int) size];
        columnSourceMap.put("SnapshotObject",
                InMemoryColumnSource.getImmutableMemoryColumnSource(snapshotArr, Snapshot.class, null));

        // Populate the column source(s)
        for (int i = 0; i < size; i++) {
            final Snapshot snapshot = snapshots.get(i);
            idArr[i] = snapshot.snapshotId();
            // Provided as millis from epoch, convert to nanos
            timestampArr[i] = DateTimeUtils.millisToNanos(snapshot.timestampMillis());
            operatorArr[i] = snapshot.operation();
            summaryArr[i] = snapshot.summary();
            snapshotArr[i] = snapshot;
        }

        // Create and return the table
        return new QueryTable(SNAPSHOT_DEFINITION, RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    /**
     * Retrieve a specific {@link Snapshot snapshot} of an Iceberg table.
     *
     * @param snapshotId The identifier of the snapshot to load.
     *
     * @return An {@link Optional} containing the requested {@link Snapshot} if it exists.
     */
    private Optional<Snapshot> snapshot(final long snapshotId) {
        Optional<Snapshot> found = getSnapshots().stream()
                .filter(snapshot -> snapshot.snapshotId() == snapshotId)
                .findFirst();
        if (found.isEmpty()) {
            // Refresh the table to update the snapshot list, then try again.
            refresh();
            found = getSnapshots().stream()
                    .filter(snapshot -> snapshot.snapshotId() == snapshotId)
                    .findFirst();
        }
        return found;
    }

    /**
     * Retrieve the current {@link Schema schema} of an Iceberg table.
     */
    public synchronized Schema currentSchema() {
        refresh();
        return table.schema();
    }

    /**
     * Retrieve the current {@link Schema schema} of an Iceberg table.
     */
    public synchronized Map<Integer, Schema> schemas() {
        refresh();
        return Map.copyOf(table.schemas());
    }

    /**
     * Retrieve a specific {@link Schema schema} of an Iceberg table.
     *
     * @param schemaId The identifier of the schema to load.
     */
    public synchronized Optional<Schema> schema(final int schemaId) {
        Schema found = table.schemas().get(schemaId);
        if (found == null) {
            // Refresh the table to update the snapshot list, then try again.
            refresh();
            found = table.schemas().get(schemaId);
        }
        return Optional.ofNullable(found);
    }

    /**
     * Retrieves the appropriate {@link Snapshot} based on the provided {@link IcebergReadInstructions}, or {@code null}
     * if no {@link IcebergReadInstructions#snapshot() snapshot} or {@link IcebergReadInstructions#snapshotId()
     * snapshotId} is provided.
     */
    @InternalUseOnly
    @Nullable
    public Snapshot getSnapshot(@NotNull final IcebergReadInstructions readInstructions) {
        if (readInstructions.snapshot().isPresent()) {
            return readInstructions.snapshot().get();
        } else if (readInstructions.snapshotId().isPresent()) {
            return snapshot(readInstructions.snapshotId().getAsLong())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Snapshot with id " + readInstructions.snapshotId().getAsLong() + " not found for " +
                                    "table " + tableIdentifier));
        }
        return null;
    }

    /**
     * The {@link TableDefinition Table definition} for this Iceberg table.
     *
     * @return The table definition
     */
    public TableDefinition definition() {
        return resolver.definition();
    }

    /**
     * The {@link TableDefinition Table definition} for this Iceberg table.
     *
     * @param readInstructions The instructions for customizations while reading the table.
     * @return The table definition
     * @deprecated use {@link #definition()}
     */
    @Deprecated
    public TableDefinition definition(@NotNull final IcebergReadInstructions readInstructions) {
        return resolver.definition();
    }

    /**
     * The {@link TableTools#metaTable(TableDefinition) metadata Table} for this Iceberg table.
     *
     * @return The table definition as a Deephaven table
     * @see #definitionTable(IcebergReadInstructions)
     */
    public Table definitionTable() {
        return TableTools.metaTable(resolver.definition());
    }

    /**
     * The {@link TableTools#metaTable(TableDefinition) metadata Table} for this Iceberg table.
     *
     * @param readInstructions The instructions for customizations while reading the table.
     * @return The table definition as a Deephaven table
     * @deprecated use {@link #definitionTable()}
     */
    @Deprecated
    public Table definitionTable(final IcebergReadInstructions readInstructions) {
        return definitionTable();
    }

    /**
     * Read the latest snapshot of this Iceberg table from the Iceberg catalog as a Deephaven {@link Table table} using
     * {@link IcebergReadInstructions#DEFAULT default read instructions}.
     *
     * <p>
     * Equivalent to {@code table(IcebergReadInstructions.DEFAULT)}.
     *
     * @return The loaded table
     * @see #table(IcebergReadInstructions)
     */
    public IcebergTable table() {
        return table(IcebergReadInstructions.DEFAULT);
    }

    /**
     * Read this Iceberg table from the Iceberg catalog as a Deephaven {@link Table table} using
     * {@code readInstructions}.
     *
     * @param readInstructions The instructions for customizations while reading the table.
     * @return The loaded table
     */
    public IcebergTable table(@NotNull final IcebergReadInstructions readInstructions) {
        refresh();
        return table(StandaloneTableKey.getInstance(), readInstructions);
    }

    /**
     * Refresh the table with the latest information from the Iceberg catalog, including new snapshots and schema.
     */
    public synchronized void refresh() {
        table.refresh();
    }

    /**
     * Return the underlying Iceberg table.
     */
    public org.apache.iceberg.Table icebergTable() {
        return table;
    }

    @Override
    public String toString() {
        return table.toString();
    }

    /**
     * Create a new {@link IcebergTableWriter} for this Iceberg table using the provided {@link TableWriterOptions}.
     * <p>
     * This method will perform schema validation to ensure that the provided
     * {@link TableWriterOptions#tableDefinition()} is compatible with the Iceberg table schema. All further writes
     * performed by the returned writer will not be validated against the table's schema, and thus will be faster.
     *
     * @param tableWriterOptions The options to configure the table writer.
     * @return A new instance of {@link IcebergTableWriter} configured with the provided options.
     */
    public IcebergTableWriter tableWriter(final TableWriterOptions tableWriterOptions) {
        return new IcebergTableWriter(tableWriterOptions, this, dataInstructionsProviderLoader);
    }

    /**
     * Get the location URI of the Iceberg table.
     */
    public URI locationUri() {
        return locationUri;
    }

    // Visible for DHE
    @InternalUseOnly
    public IcebergTable table(
            @NotNull final TableKey tableKey,
            @NotNull final IcebergReadInstructions readInstructions) {
        final IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> p =
                provider(tableKey, readInstructions);
        if (p instanceof IcebergStaticTableLocationProvider) {
            return new IcebergTableImpl(
                    resolver.definition(),
                    tableIdentifier.toString(),
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    p,
                    null);
        }
        if (p instanceof IcebergManualRefreshTableLocationProvider
                || p instanceof IcebergAutoRefreshTableLocationProvider) {
            return new IcebergTableImpl(
                    resolver.definition(),
                    tableIdentifier.toString(),
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    p,
                    ExecutionContext.getContext().getUpdateGraph());
        }
        throw new IllegalStateException("Unexpected TableLocationProvider: " + p.getClass().getName());
    }

    // Visible for DHE
    @InternalUseOnly
    public IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> provider(
            @NotNull final TableKey tableKey,
            @NotNull final IcebergReadInstructions readInstructions) {
        // Core+ will use this, as their extended format is based on TLPs hooks instead of Table hooks.
        final Snapshot snapshot = snapshot(readInstructions);
        final IcebergBaseLayout keyFinder = keyFinder(
                snapshot,
                readInstructions.dataInstructions().orElse(null),
                readInstructions.ignoreResolvingErrors());
        if (readInstructions.updateMode().updateType() == IcebergUpdateMode.IcebergUpdateType.STATIC) {
            return new IcebergStaticTableLocationProvider<>(
                    tableKey,
                    keyFinder,
                    new IcebergTableLocationFactory(),
                    tableIdentifier);
        }
        if (readInstructions.updateMode().updateType() == IcebergUpdateMode.IcebergUpdateType.MANUAL_REFRESHING) {
            return new IcebergManualRefreshTableLocationProvider<>(
                    tableKey,
                    keyFinder,
                    new IcebergTableLocationFactory(),
                    this,
                    tableIdentifier);
        } else {
            return new IcebergAutoRefreshTableLocationProvider<>(
                    tableKey,
                    keyFinder,
                    new IcebergTableLocationFactory(),
                    TableDataRefreshService.getSharedRefreshService(),
                    readInstructions.updateMode().autoRefreshMs(),
                    this,
                    tableIdentifier);
        }
    }

    private Snapshot snapshot(@NotNull final IcebergReadInstructions readInstructions) {
        final Snapshot explicitSnapshot = getSnapshot(readInstructions);
        return explicitSnapshot == null
                ? table.currentSnapshot()
                : explicitSnapshot;
    }

    private @NotNull IcebergBaseLayout keyFinder(
            @Nullable final Snapshot snapshot,
            @Nullable final Object dataInstructions,
            final boolean ignoreResolvingErrors) {
        final Object specialInstructions;
        final SeekableChannelsProvider channelsProvider;
        {
            final String uriScheme = locationUri.getScheme();
            specialInstructions = dataInstructions == null
                    ? dataInstructionsProviderLoader.load(uriScheme)
                    : dataInstructions;
            channelsProvider = seekableChannelsProvider(uriScheme, specialInstructions);
        }
        final ParquetInstructions parquetInstructions = ParquetInstructions.builder()
                .setTableDefinition(resolver.definition())
                .setColumnResolverFactory(new ResolverFactory(resolver, nameMapping, ignoreResolvingErrors))
                .setSpecialInstructions(specialInstructions)
                .build();
        final Map<String, PartitionField> partitionFields = resolver.partitionFieldMap();
        if (partitionFields.isEmpty()) {
            return new IcebergUnpartitionedLayout(this, parquetInstructions, channelsProvider, snapshot);
        }
        return new IcebergPartitionedLayout(this, parquetInstructions, channelsProvider, snapshot, resolver);
    }

    private static SeekableChannelsProvider seekableChannelsProvider(
            final String uriScheme,
            final Object specialInstructions) {
        final SeekableChannelsProviderLoader loader = SeekableChannelsProviderLoader.getInstance();
        return S3_SCHEMES.contains(uriScheme)
                ? loader.load(S3_SCHEMES, specialInstructions)
                : loader.load(uriScheme, specialInstructions);
    }
}
