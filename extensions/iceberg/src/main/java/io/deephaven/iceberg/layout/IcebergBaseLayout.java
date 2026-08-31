//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.location.IcebergTableParquetLocationKey;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static io.deephaven.iceberg.base.IcebergUtils.dataFileUri;

@InternalUseOnly
public abstract class IcebergBaseLayout implements TableLocationKeyFinder<IcebergTableLocationKey> {

    private static final Logger log = LoggerFactory.getLogger(IcebergBaseLayout.class);

    /**
     * Whether {@link IcebergReadInstructions#pruningExpression() pruning expression} field references are matched
     * case-sensitively. Hard-coded to Iceberg's scan default; may become configurable.
     */
    public static final boolean PRUNING_CASE_SENSITIVE = true;

    /**
     * The {@link IcebergTableAdapter} that will be used to access the table.
     */
    protected final IcebergTableAdapter tableAdapter;

    /**
     * The UUID of the table, if available.
     */
    @Nullable
    private final UUID tableUuid;

    /**
     * Name of the {@link Catalog} used to access this table, if available.
     */
    @Nullable
    private final String catalogName;

    /**
     * The table identifier used to access this table.
     */
    private final TableIdentifier tableIdentifier;

    /**
     * The {@link TableDefinition} that will be used for life of this table. Although Iceberg table schema may change,
     * schema changes are not supported in Deephaven.
     */
    @Deprecated(forRemoval = true)
    final TableDefinition tableDef;

    /**
     * The {@link Snapshot} from which to discover data files.
     */
    Snapshot snapshot;

    /**
     * The {@link ParquetInstructions} object that will be used to read any Parquet data files in this table.
     */
    private final ParquetInstructions parquetInstructions;

    /**
     * The {@link SeekableChannelsProvider} object that will be used for {@link IcebergTableParquetLocationKey}
     * creation.
     */
    private final SeekableChannelsProvider seekableChannelsProvider;

    /**
     * The {@link IcebergReadInstructions#pruningExpression() pruning expression}.
     */
    @NotNull
    private final Expression pruningExpression;

    /**
     * Memoized {@link ManifestEvaluator ManifestEvaluators} by {@link PartitionSpec} id, where {@code null} means
     * "cannot prune this spec".
     */
    private final Map<Integer, ManifestEvaluator> manifestEvaluators = new HashMap<>();

    /**
     * Create a new {@link IcebergTableLocationKey} for the given {@link ManifestFile}, {@link DataFile} and
     * {@link URI}.
     *
     * @param manifestPartitionSpec The {@link PartitionSpec} that applies to the manifest file from which the data file
     *        was discovered
     * @param manifestFile The manifest file from which the data file was discovered
     * @param dataFile The data file that backs the keyed location
     * @param fileUri The {@link URI} for the file that backs the keyed location
     * @param partitions The table partitions enclosing the table location keyed by the returned key. If {@code null},
     *        the location will be a member of no partitions.
     *
     * @return A new {@link IcebergTableLocationKey}
     */
    protected IcebergTableLocationKey locationKey(
            @NotNull final PartitionSpec manifestPartitionSpec,
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        final org.apache.iceberg.FileFormat format = dataFile.format();
        if (format == org.apache.iceberg.FileFormat.PARQUET) {
            return new IcebergTableParquetLocationKey(catalogName, tableUuid, tableIdentifier, manifestPartitionSpec,
                    manifestFile, dataFile,
                    fileUri, 0, partitions, parquetInstructions, channelsProvider,
                    computeSortedColumns(tableAdapter.icebergTable(), dataFile, parquetInstructions));
        }
        throw new UnsupportedOperationException(String.format("%s:%d - an unsupported file format %s for URI '%s'",
                tableAdapter, snapshot.snapshotId(), format, fileUri));
    }

    /**
     * @param tableAdapter The {@link IcebergTableAdapter} that will be used to access the table.
     * @param instructions The instructions for customizations while reading.
     * @param dataInstructionsProvider The provider for special instructions, to be used if special instructions not
     *        provided in the {@code instructions}.
     */
    @Deprecated
    public IcebergBaseLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final IcebergReadInstructions instructions,
            @NotNull final DataInstructionsProviderLoader dataInstructionsProvider) {
        this.tableAdapter = tableAdapter;
        {
            UUID uuid;
            try {
                uuid = tableAdapter.icebergTable().uuid();
            } catch (final RuntimeException e) {
                // The UUID method is unsupported for v1 Iceberg tables since uuid is optional for v1 tables.
                uuid = null;
            }
            this.tableUuid = uuid;
        }

        this.catalogName = tableAdapter.catalog().name();
        this.tableIdentifier = tableAdapter.tableIdentifier();

        this.snapshot = tableAdapter.getSnapshot(instructions);
        this.tableDef = tableAdapter.definition(instructions);
        // Set to Expressions.alwaysTrue() if no pruning is requested.
        this.pruningExpression = instructions.pruningExpression();

        final String uriScheme = tableAdapter.locationUri().getScheme();
        // Add the data instructions if provided as part of the IcebergReadInstructions, or else attempt to create
        // data instructions from the properties collection and URI scheme.
        final Object specialInstructions = instructions.dataInstructions()
                .orElseGet(() -> dataInstructionsProvider.load(uriScheme));
        {
            // Start with user-supplied instructions (if provided).
            final ParquetInstructions.Builder builder = new ParquetInstructions.Builder();

            // Add the table definition.
            builder.setTableDefinition(tableDef);

            // Add any column rename mappings.
            if (!instructions.columnRenames().isEmpty()) {
                for (Map.Entry<String, String> entry : instructions.columnRenames().entrySet()) {
                    builder.addColumnNameMapping(entry.getKey(), entry.getValue());
                }
            }
            if (specialInstructions != null) {
                builder.setSpecialInstructions(specialInstructions);
            }
            this.parquetInstructions = builder.build();
        }

        if ("s3".equals(uriScheme) || "s3a".equals(uriScheme) || "s3n".equals(uriScheme)) {
            seekableChannelsProvider =
                    SeekableChannelsProviderLoader.getInstance().load(Set.of("s3", "s3a", "s3n"), specialInstructions);
        } else {
            seekableChannelsProvider =
                    SeekableChannelsProviderLoader.getInstance().load(uriScheme, specialInstructions);
        }
    }

    protected IcebergBaseLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final ParquetInstructions parquetInstructions,
            @NotNull final SeekableChannelsProvider seekableChannelsProvider,
            @Nullable final Snapshot snapshot) {
        this(tableAdapter, parquetInstructions, seekableChannelsProvider, snapshot, Expressions.alwaysTrue());
    }

    protected IcebergBaseLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final ParquetInstructions parquetInstructions,
            @NotNull final SeekableChannelsProvider seekableChannelsProvider,
            @Nullable final Snapshot snapshot,
            @NotNull final Expression pruningExpression) {
        this.tableAdapter = Objects.requireNonNull(tableAdapter);
        {
            UUID uuid;
            try {
                uuid = tableAdapter.icebergTable().uuid();
            } catch (final RuntimeException e) {
                // The UUID method is unsupported for v1 Iceberg tables since uuid is optional for v1 tables.
                uuid = null;
            }
            this.tableUuid = uuid;
        }
        this.catalogName = tableAdapter.catalog().name();
        this.tableIdentifier = tableAdapter.tableIdentifier();
        this.parquetInstructions = Objects.requireNonNull(parquetInstructions);
        this.seekableChannelsProvider = Objects.requireNonNull(seekableChannelsProvider);
        this.snapshot = snapshot;
        // Set to Expressions.alwaysTrue() if no pruning is requested.
        this.pruningExpression = Objects.requireNonNull(pruningExpression);
        // not used in the updated constructors' path
        this.tableDef = null;
    }

    private boolean pruningEnabled() {
        // Expressions.alwaysTrue() is a singleton, so a reference comparison is sufficient.
        return pruningExpression != Expressions.alwaysTrue();
    }

    protected abstract IcebergTableLocationKey keyFromDataFile(
            PartitionSpec manifestPartitionSpec,
            ManifestFile manifestFile,
            DataFile dataFile,
            URI fileUri,
            SeekableChannelsProvider channelsProvider);

    private IcebergTableLocationKey key(
            final Table table,
            final PartitionSpec manifestPartitionSpec,
            final ManifestFile manifestFile,
            final ManifestReader<?> ignoredManifestReader,
            final DataFile dataFile) {
        // Note: ManifestReader explicitly modelled in this path, as it can contain relevant information.
        // ie, ManifestReader.spec(), ManifestReader.spec().schema()
        // See https://lists.apache.org/thread/88md2fdk17k26cl4gj3sz6sdbtwcgbk5
        final URI fileUri = dataFileUri(table, dataFile);
        return keyFromDataFile(manifestPartitionSpec, manifestFile, dataFile, fileUri, seekableChannelsProvider);
    }

    private static void checkIsDataManifest(ManifestFile manifestFile) {
        if (manifestFile.content() != ManifestContent.DATA) {
            throw new UnsupportedOperationException(String.format(
                    "only DATA manifest files are currently supported, encountered %s", manifestFile.content()));
        }
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<IcebergTableLocationKey> locationKeyObserver) {
        if (snapshot == null) {
            return;
        }
        final Table table = tableAdapter.icebergTable();
        int skippedManifests = 0;
        int unprunedManifests = 0;
        int acceptedDataFiles = 0;
        try {
            final FileIO io = table.io();
            final List<ManifestFile> manifestFiles = snapshot.allManifests(io);
            // Deliberately unconditional and ahead of pruning: an unsupported manifest must be reported even if
            // pruning would have skipped it.
            for (final ManifestFile manifestFile : manifestFiles) {
                checkIsDataManifest(manifestFile);
            }
            for (final ManifestFile manifestFile : manifestFiles) {
                final ManifestEvaluator manifestEvaluator =
                        pruningEnabled() ? manifestEvaluator(table, manifestFile) : null;
                if (manifestEvaluator != null && !manifestEvaluator.eval(manifestFile)) {
                    // The partition summaries prove this manifest holds no matching data files, so skip it unread
                    ++skippedManifests;
                    continue;
                }
                try (final ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifestFile, io, null)) {
                    final PartitionSpec manifestPartitionSpec = manifestReader.spec();
                    if (pruningEnabled()) {
                        // A manifest embeds the schema that was current when it was written, and that is what
                        // filterRows binds against; it may pre-date fields the expression references.
                        if (canBind(manifestPartitionSpec.schema())) {
                            manifestReader.filterRows(pruningExpression).caseSensitive(PRUNING_CASE_SENSITIVE);
                        } else {
                            ++unprunedManifests;
                            log.warn().append(toString())
                                    .append(": pruning expression ")
                                    .append(ExpressionUtil.toSanitizedString(pruningExpression))
                                    .append(" cannot be bound against the schema of manifest '")
                                    .append(manifestFile.path())
                                    .append("'; reading it in full")
                                    .endl();
                        }
                    }
                    for (final DataFile dataFile : manifestReader) {
                        ++acceptedDataFiles;
                        locationKeyObserver
                                .accept(key(table, manifestPartitionSpec, manifestFile, manifestReader, dataFile));
                    }
                }
            }
        } catch (RuntimeException | IOException e) {
            throw new TableDataException(
                    String.format("%s:%d - error finding Iceberg locations", tableAdapter, snapshot.snapshotId()), e);
        }
        if (pruningEnabled()) {
            log.info().append(toString())
                    .append(": pruning expression ")
                    .append(ExpressionUtil.toSanitizedString(pruningExpression))
                    .append(" skipped ").append(skippedManifests)
                    .append(" manifest(s), could not be applied to ").append(unprunedManifests)
                    .append(" manifest(s), and accepted ").append(acceptedDataFiles)
                    .append(" data file(s)")
                    .endl();
        }
    }

    /**
     * Whether {@link #pruningExpression} can be bound against {@code schema}.
     */
    private boolean canBind(@NotNull final Schema schema) {
        try {
            Binder.bind(schema.asStruct(), pruningExpression, PRUNING_CASE_SENSITIVE);
            return true;
        } catch (final ValidationException e) {
            return false;
        }
    }

    /**
     * Get the {@link ManifestEvaluator} for {@code manifestFile}'s {@link PartitionSpec}, or {@code null} if
     * {@link #pruningExpression} cannot be projected onto that spec. Declining to prune is always correct, since
     * pruning is only ever an optimization.
     */
    @Nullable
    private ManifestEvaluator manifestEvaluator(
            @NotNull final Table table,
            @NotNull final ManifestFile manifestFile) {
        final int specId = manifestFile.partitionSpecId();
        if (manifestEvaluators.containsKey(specId)) {
            return manifestEvaluators.get(specId);
        }
        ManifestEvaluator evaluator = null;
        final PartitionSpec spec = table.specs().get(specId);
        if (spec == null) {
            log.warn().append(toString())
                    .append(": partition spec id ").append(specId)
                    .append(" from manifest '").append(manifestFile.path())
                    .append("' is not present in the table metadata; not pruning manifests using this spec")
                    .endl();
        } else {
            try {
                evaluator = ManifestEvaluator.forRowFilter(pruningExpression, spec, PRUNING_CASE_SENSITIVE);
            } catch (final ValidationException e) {
                log.warn().append(toString())
                        .append(": pruning expression ")
                        .append(ExpressionUtil.toSanitizedString(pruningExpression))
                        .append(" cannot be bound against partition spec id ").append(specId)
                        .append("; not pruning manifests using this spec: ").append(e.getMessage())
                        .endl();
            }
        }
        // Note: a null value is memoized, so we warn at most once per spec per layout.
        manifestEvaluators.put(specId, evaluator);
        return evaluator;
    }

    /**
     * Update the snapshot to the latest snapshot from the catalog if
     */
    protected synchronized boolean maybeUpdateSnapshot() {
        final Snapshot latestSnapshot = tableAdapter.currentSnapshot();
        if (latestSnapshot == null) {
            return false;
        }
        if (snapshot == null || latestSnapshot.sequenceNumber() > snapshot.sequenceNumber()) {
            snapshot = latestSnapshot;
            return true;
        }
        return false;
    }

    /**
     * Update the snapshot to the user specified snapshot. See
     * {@link io.deephaven.iceberg.util.IcebergTable#update(long)} for more details.
     */
    protected void updateSnapshot(long snapshotId) {
        final List<Snapshot> snapshots = tableAdapter.listSnapshots();

        final Snapshot snapshot = snapshots.stream()
                .filter(s -> s.snapshotId() == snapshotId).findFirst()
                .orElse(null);

        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Snapshot " + snapshotId + " was not found in the list of snapshots for table " + tableAdapter
                            + ". Snapshots: " + snapshots);
        }
        updateSnapshot(snapshot);
    }

    /**
     * Update the snapshot to the user specified snapshot. See
     * {@link io.deephaven.iceberg.util.IcebergTable#update(Snapshot)} for more details.
     */
    protected void updateSnapshot(@NotNull final Snapshot updateSnapshot) {
        // Validate that we are not trying to update to an older snapshot.
        if (snapshot != null && updateSnapshot.sequenceNumber() <= snapshot.sequenceNumber()) {
            throw new IllegalArgumentException(
                    "Update snapshot sequence number (" + updateSnapshot.sequenceNumber()
                            + ") must be higher than the current snapshot sequence number ("
                            + snapshot.sequenceNumber() + ") for table " + tableAdapter);
        }

        snapshot = updateSnapshot;
    }

    @VisibleForTesting
    @NotNull
    public static List<SortColumn> computeSortedColumns(
            @NotNull final org.apache.iceberg.Table icebergTable,
            @NotNull final DataFile dataFile,
            @NotNull final ParquetInstructions readInstructions) {
        final Integer sortOrderId = dataFile.sortOrderId();
        // If sort order is missing or unknown, we cannot determine the sorted columns from the metadata and will
        // check the underlying parquet file for the sorted columns, when the user asks for them.
        if (sortOrderId == null) {
            return Collections.emptyList();
        }
        final SortOrder sortOrder = icebergTable.sortOrders().get(sortOrderId);
        if (sortOrder == null) {
            return Collections.emptyList();
        }
        if (sortOrder.isUnsorted()) {
            return Collections.emptyList();
        }
        final Schema schema = sortOrder.schema();
        final List<SortColumn> sortColumns = new ArrayList<>(sortOrder.fields().size());
        for (final SortField field : sortOrder.fields()) {
            if (!field.transform().isIdentity()) {
                // TODO (DH-18160): Improve support for handling non-identity transforms
                break;
            }
            final String icebergColName = schema.findColumnName(field.sourceId());
            final String dhColName = readInstructions.getColumnNameFromParquetColumnNameOrDefault(icebergColName);
            final TableDefinition tableDefinition = readInstructions.getTableDefinition().orElseThrow(
                    () -> new IllegalStateException("Table definition is required for reading from Iceberg tables"));
            final ColumnDefinition<?> columnDef = tableDefinition.getColumn(dhColName);
            if (columnDef == null) {
                // Table definition provided by the user doesn't have this column, so stop here
                break;
            }
            final SortColumn sortColumn;
            if (field.nullOrder() == NullOrder.NULLS_FIRST && field.direction() == SortDirection.ASC) {
                sortColumn = SortColumn.asc(ColumnName.of(dhColName));
            } else if (field.nullOrder() == NullOrder.NULLS_LAST && field.direction() == SortDirection.DESC) {
                sortColumn = SortColumn.desc(ColumnName.of(dhColName));
            } else {
                break;
            }
            sortColumns.add(sortColumn);
        }
        return Collections.unmodifiableList(sortColumns);
    }
}
