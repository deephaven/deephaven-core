//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class S3MetadataTableTestBootstrap {

    public static void main(final String[] args) throws IOException {
        // Note: see comment in build.gradle about devS3Tables property - you may need additional items on your
        // classpath to reference S3TablesCatalog. You will also need to make sure the current environment is setup
        // with the proper auth. The easiest way is to set the environment variable `AWS_PROFILE=your-profile`.
        final Map<String, String> properties = Map.of(
                "catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog",
                "warehouse", "arn:aws:s3tables:us-east-2:911167914844:bucket/deephaven-s3-metadata");
        final Catalog catalog = CatalogUtil.buildIcebergCatalog("deephaven-s3-metadata", properties, null);
        final Table s3MetadataTable =
                catalog.loadTable(TableIdentifier.of("aws_s3_metadata", "s3metadata_deephaven_docs"));
        final String prettyJson = SchemaParser.toJson(s3MetadataTable.schema(), true);
        final Path path = Path.of(
                "/path/to/deephaven-core/extensions/iceberg/src/test/resources/io/deephaven/iceberg/internal/s3_metadata_schema.json");
        Files.writeString(path, prettyJson);
    }
}
