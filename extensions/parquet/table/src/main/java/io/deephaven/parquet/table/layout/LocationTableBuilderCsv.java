//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.base.FileUtils;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.local.KeyValuePartitionLayout;
import io.deephaven.engine.util.TableTools;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * {@link KeyValuePartitionLayout.LocationTableBuilder LocationTableBuilder} implementation that uses the default
 * Deephaven CSV parser for type inference and coercion.
 */
public final class LocationTableBuilderCsv implements KeyValuePartitionLayout.LocationTableBuilder {

    private static final String LS = System.lineSeparator();

    private final URI tableRootDirectory;

    private List<String> partitionKeys;
    private StringBuilder csvBuilder;
    private int locationCount;

    public LocationTableBuilderCsv(@NotNull final File tableRootDirectory) {
        this(FileUtils.convertToURI(tableRootDirectory, true));
    }

    LocationTableBuilderCsv(@NotNull final URI tableRootDirectory) {
        this.tableRootDirectory = tableRootDirectory;
    }

    @Override
    public void registerPartitionKeys(@NotNull final Collection<String> partitionKeys) {
        if (this.partitionKeys != null) {
            throw new IllegalStateException("Partition keys already registered");
        }
        this.partitionKeys = List.copyOf(Objects.requireNonNull(partitionKeys));
        if (this.partitionKeys.isEmpty()) {
            return;
        }
        csvBuilder = new StringBuilder();
        maybeAppendToCsv(partitionKeys);
    }

    @Override
    public void acceptLocation(@NotNull final Collection<String> partitionValueStrings) {
        if (partitionKeys == null) {
            throw new IllegalStateException("Partition keys not registered");
        }
        ++locationCount;
        maybeAppendToCsv(partitionValueStrings);
    }

    private void maybeAppendToCsv(@NotNull final Collection<String> values) {
        if (partitionKeys.isEmpty()) {
            return;
        }
        boolean first = true;
        for (final String value : values) {
            if (first) {
                first = false;
            } else {
                csvBuilder.append(',');
            }
            csvBuilder.append(StringEscapeUtils.escapeCsv(value));
        }
        csvBuilder.append(LS);
    }

    @Override
    public Table build() {
        try {
            return partitionKeys == null || partitionKeys.isEmpty()
                    ? TableTools.emptyTable(locationCount)
                    : CsvTools.readCsv(new ByteArrayInputStream(csvBuilder.toString().getBytes()));
        } catch (CsvReaderException e) {
            throw new TableDataException("Failed converting partition CSV to table for " + tableRootDirectory, e);
        }
    }
}
