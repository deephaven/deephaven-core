package io.deephaven.iceberg.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.locations.impl.KnownLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.impl.PollingTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.layout.IcebergFlatLayout;
import io.deephaven.iceberg.layout.IcebergPartitionedLayout;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergCatalog {
    private final IcebergInstructions instructions;
    private final Catalog catalog;
    private final FileIO fileIO;

    private final S3Instructions s3Instructions;

    /**
     * Construct an IcebergCatalog given a set of configurable instructions..
     *
     * @param name The optional service name
     */
    IcebergCatalog(final @Nullable String name, final IcebergInstructions instructions) {
        this.instructions = instructions;

        // Set up the properties map for the Iceberg catalog
        Map<String, String> properties = new HashMap<>();

        final Configuration conf = new Configuration();

        properties.put(CatalogProperties.CATALOG_IMPL, instructions.catalogImpl().value);
        if (instructions.catalogImpl() == IcebergInstructions.CATALOG_IMPL.RESTCatalog) {
            final RESTCatalog restCatalog = new RESTCatalog();
            restCatalog.setConf(conf);
            catalog = restCatalog;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported catalog implementation: " + instructions.catalogImpl());
        }

        properties.put(CatalogProperties.URI, instructions.catalogURI());
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, instructions.warehouseLocation());

        properties.put(CatalogProperties.FILE_IO_IMPL, instructions.fileIOImpl().value);
        if (instructions.fileIOImpl() == IcebergInstructions.FILEIO_IMPL.S3) {
            final S3Instructions.Builder builder = S3Instructions.builder()
                    .credentials(Credentials.basic(instructions.s3AccessKeyId(), instructions.s3SecretAccessKey()))
                    .regionName(instructions.s3Region());

            properties.put(AwsClientProperties.CLIENT_REGION, instructions.s3Region());
            properties.put(S3FileIOProperties.ACCESS_KEY_ID, instructions.s3AccessKeyId());
            properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, instructions.s3SecretAccessKey());
            if (instructions.s3EndpointOverride().isPresent()) {
                properties.put(S3FileIOProperties.ENDPOINT, instructions.s3EndpointOverride().get());
                builder.endpointOverride(instructions.s3EndpointOverride().get());
            }
            s3Instructions = builder.build();
            // TODO: create a FileIO interface wrapping the Deephaven S3SeekableByteChannel/Provider
            fileIO = CatalogUtil.loadFileIO(instructions.fileIOImpl().value, properties, conf);
        } else {
            throw new UnsupportedOperationException("Unsupported file IO implementation: " + instructions.fileIOImpl());
        }

        final String catalogName = name != null ? name : "IcebergTableDataService-" + instructions.catalogURI();
        catalog.initialize(catalogName, properties);
    }

    public List<TableIdentifier> listTables(final Namespace namespace) {
        // TODO: have this return a Deephaven Table of table identifiers
        return catalog.listTables(namespace);
    }

    /**
     * Read a static snapshot of a table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param snapshotId The snapshot ID to load
     * @return The loaded table
     */
    public Table readTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final String snapshotId) {
        return readTableInternal(tableIdentifier, snapshotId, false);
    }

    /**
     * Read the latest static snapshot of a table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @return The loaded table
     */
    public Table readTable(@NotNull final TableIdentifier tableIdentifier) {
        return readTableInternal(tableIdentifier, null, false);
    }

    /**
     * Subscribe to a table from the Iceberg catalog. Intially the latest snapshot will be loaded, but the output table
     * will be updated as new snapshots are added to the table.
     *
     * @param tableIdentifier The table identifier to load
     * @return The loaded table
     */
    public Table subscribeTable(@NotNull final TableIdentifier tableIdentifier) {
        return readTableInternal(tableIdentifier, null, true);
    }

    private static KnownLocationKeyFinder<IcebergTableLocationKey> toKnownKeys(
            TableLocationKeyFinder<IcebergTableLocationKey> keyFinder) {
        return keyFinder instanceof KnownLocationKeyFinder
                ? (KnownLocationKeyFinder<IcebergTableLocationKey>) keyFinder
                : KnownLocationKeyFinder.copyFrom(keyFinder, Comparator.naturalOrder());
    }

    private Table readTableInternal(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final String snapshotId,
            final boolean isRefreshing) {
        // Validate that the user is not trying to subscribe to a snapshot.
        Assert.eqFalse(isRefreshing && (snapshotId != null), "Must not specify a snapshot ID when subscribing to a table.");

        // Load the table from the catalog
        final org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);

        final Snapshot snapshot;
        final Schema schema;
        // Do we want the latest or a specific snapshot?
        if (snapshotId == null) {
            snapshot = table.currentSnapshot();
            schema = table.schema();
        } else {
            // Load the specific snapshot and retrieve the schema for that snapshot
            snapshot = table.snapshot(snapshotId);
            schema = table.schemas().get(snapshot.schemaId());
        }

        // Load the partitioning schema
        final org.apache.iceberg.PartitionSpec partitionSpec = table.spec();

        // Convert the Iceberg schema to a Deephaven TableDefinition
        final TableDefinition tableDefinition = IcebergTools.fromSchema(schema, partitionSpec);

        // Build a parquet instructions object
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setSpecialInstructions(s3Instructions)
                .build();

        final String description;
        final TableLocationKeyFinder<IcebergTableLocationKey> keyFinder;
        final TableDataRefreshService refreshService;
        final UpdateSourceRegistrar updateSourceRegistrar;

        if (partitionSpec.isUnpartitioned()) {
            // Create the flat layout location key finder
            final TableLocationKeyFinder<IcebergTableLocationKey> locationKeyFinder
                    = new IcebergFlatLayout(snapshot, fileIO, instructions);

            if (isRefreshing) {
                keyFinder = locationKeyFinder;
                description = "Read refreshing iceberg table with " + keyFinder;
                refreshService = TableDataRefreshService.getSharedRefreshService();
                updateSourceRegistrar = ExecutionContext.getContext().getUpdateGraph();
            } else {
                keyFinder = toKnownKeys(locationKeyFinder);
                description = "Read static iceberg table with " + keyFinder;
                refreshService = null;
                updateSourceRegistrar = null;
            }

            return new PartitionAwareSourceTable(
                    tableDefinition,
                    description,
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    new PollingTableLocationProvider<>(
                            StandaloneTableKey.getInstance(),
                            keyFinder,
                            new IcebergTableLocationFactory(instructions),
                            refreshService),
                    updateSourceRegistrar);
        } else {
            final String[] partitionColumns =
                    partitionSpec.fields().stream().map(PartitionField::name).toArray(String[]::new);

            // Create the partitioning column location key finder
            final TableLocationKeyFinder<IcebergTableLocationKey> locationKeyFinder = new IcebergPartitionedLayout(
                    snapshot,
                    fileIO,
                    partitionColumns,
                    instructions);

            if (isRefreshing) {
                keyFinder = locationKeyFinder;
                description = "Read refreshing iceberg table with " + keyFinder;
                refreshService = TableDataRefreshService.getSharedRefreshService();
                updateSourceRegistrar = ExecutionContext.getContext().getUpdateGraph();
            } else {
                keyFinder = toKnownKeys(locationKeyFinder);
                description = "Read static iceberg table with " + keyFinder;
                refreshService = null;
                updateSourceRegistrar = null;
            }

            return new PartitionAwareSourceTable(
                    tableDefinition,
                    description,
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    new PollingTableLocationProvider<>(
                            StandaloneTableKey.getInstance(),
                            keyFinder,
                            new IcebergTableLocationFactory(instructions),
                            refreshService),
                    updateSourceRegistrar);
        }
    }


    /**
     * Return the internal Iceberg catalog.
     */
    public Catalog catalog() {
        return catalog;
    }
}
