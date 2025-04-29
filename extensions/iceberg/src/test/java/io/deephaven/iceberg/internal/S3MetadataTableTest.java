//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.util.TypeInference;
import io.deephaven.iceberg.util.InferenceInstructions;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Objects;

import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static org.assertj.core.api.Assertions.assertThat;

class S3MetadataTableTest {

    /**
     * This schema was captured directly from a "software.amazon.s3tables.iceberg.S3TablesCatalog" Catalog against an
     * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/metadata-tables-schema.html">S3 Metadata
     * Table</a>.
     *
     * <pre>
     *   org.apache.iceberg.catalog.Catalog catalog = ...;
     *   org.apache.iceberg.Table s3MetadataTable = catalog.loadTable(...);
     *   System.out.println(org.apache.iceberg.SchemaParser.toJson(s3MetadataTable.schema()));
     * </pre>
     */
    private static Schema s3MetadataSchema() throws IOException {
        final String json;
        try (final InputStream in = S3MetadataTableTest.class.getResourceAsStream("s3_metadata_schema.json")) {
            json = new String(Objects.requireNonNull(in).readAllBytes(), StandardCharsets.UTF_8);
        }
        return SchemaParser.fromJson(json);
    }

    @Test
    void inference() throws IOException, TypeInference.UnsupportedType {
        final Schema schema = s3MetadataSchema();
        // TODO: add description to ColumnDefinition?
        assertThat(Resolver.infer(InferenceInstructions.of(schema))).isEqualTo(Resolver.builder()
                .schema(schema)
                .definition(TableDefinition.of(
                        ColumnDefinition.ofString("bucket"),
                        ColumnDefinition.ofString("key"),
                        ColumnDefinition.ofString("sequence_number"),
                        ColumnDefinition.ofString("record_type"),
                        ColumnDefinition.of("record_timestamp", Type.find(LocalDateTime.class)),
                        ColumnDefinition.ofString("version_id"),
                        ColumnDefinition.ofBoolean("is_delete_marker"),
                        ColumnDefinition.ofLong("size"),
                        ColumnDefinition.of("last_modified_date", Type.find(LocalDateTime.class)),
                        ColumnDefinition.ofString("e_tag"),
                        ColumnDefinition.ofString("storage_class"),
                        ColumnDefinition.ofBoolean("is_multipart"),
                        ColumnDefinition.ofString("encryption_status"),
                        ColumnDefinition.ofBoolean("is_bucket_key_enabled"),
                        ColumnDefinition.ofString("kms_key_arn"),
                        ColumnDefinition.ofString("checksum_algorithm"),
                        ColumnDefinition.ofString("requester"),
                        ColumnDefinition.ofString("source_ip_address"),
                        ColumnDefinition.ofString("request_id")))
                .putColumnInstructions("bucket", schemaField(1))
                .putColumnInstructions("key", schemaField(2))
                .putColumnInstructions("sequence_number", schemaField(3))
                .putColumnInstructions("record_type", schemaField(4))
                .putColumnInstructions("record_timestamp", schemaField(5))
                .putColumnInstructions("version_id", schemaField(6))
                .putColumnInstructions("is_delete_marker", schemaField(7))
                .putColumnInstructions("size", schemaField(8))
                .putColumnInstructions("last_modified_date", schemaField(9))
                .putColumnInstructions("e_tag", schemaField(10))
                .putColumnInstructions("storage_class", schemaField(11))
                .putColumnInstructions("is_multipart", schemaField(12))
                .putColumnInstructions("encryption_status", schemaField(13))
                .putColumnInstructions("is_bucket_key_enabled", schemaField(14))
                .putColumnInstructions("kms_key_arn", schemaField(15))
                .putColumnInstructions("checksum_algorithm", schemaField(16))
                // .putColumnInstructions("object_tags", schemaField(17))
                // .putColumnInstructions("user_metadata", schemaField(18))
                .putColumnInstructions("requester", schemaField(19))
                .putColumnInstructions("source_ip_address", schemaField(20))
                .putColumnInstructions("request_id", schemaField(21))
                .build());
    }
}
