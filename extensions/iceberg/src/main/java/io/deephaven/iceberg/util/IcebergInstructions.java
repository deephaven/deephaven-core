//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * This class provides instructions intended for reading Iceberg catalogs and tables. The default values documented in
 * this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@BuildableStyle
public abstract class IcebergInstructions {
    public static Builder builder() {
        return ImmutableIcebergInstructions.builder();
    }

    public enum CATALOG_IMPL {
        RESTCatalog("org.apache.iceberg.rest.RESTCatalog");

        public final String value;

        CATALOG_IMPL(String label) {
            this.value = label;
        }
    }

    public enum FILEIO_IMPL {
        S3("org.apache.iceberg.aws.s3.S3FileIO");

        public final String value;

        FILEIO_IMPL(String label) {
            this.value = label;
        }
    }

    private final static CATALOG_IMPL DEFAULT_CATALOG_IMPL = CATALOG_IMPL.RESTCatalog;
    private final static FILEIO_IMPL DEFAULT_FILEIO_IMPL = FILEIO_IMPL.S3;

    /**
     * A custom Catalog implementation to use by an engine.
     */
    @Default
    public CATALOG_IMPL catalogImpl() {
        return DEFAULT_CATALOG_IMPL;
    }

    /**
     * The URI string for the catalog.
     */
    public abstract String catalogURI();

    /**
     * The root path of the data warehouse, where the manifest and data files are stored.
     */
    public abstract String warehouseLocation();

    /**
     * The custom FileIO implementation to use in the catalog.
     */
    @Default
    public FILEIO_IMPL fileIOImpl() {
        return DEFAULT_FILEIO_IMPL;
    }

    /**
     * The endpoint to connect to. Callers connecting to AWS do not typically need to set this; it is most useful when
     * connecting to non-AWS, S3-compatible APIs.
     *
     * @see <a href="https://docs.aws.amazon.com/general/latest/gr/s3.html">Amazon Simple Storage Service endpoints</a>
     */
    public abstract Optional<String> s3EndpointOverride();

    /**
     * The AWS access key, used to identify the user interacting with services.
     */
    public abstract String s3AccessKeyId();

    /**
     * The AWS secret access key, used to authenticate the user interacting with services.
     */
    public abstract String s3SecretAccessKey();

    /**
     * The AWS region to use for this connection.
     */
    public abstract String s3Region();

    public interface Builder {
        Builder catalogImpl(CATALOG_IMPL catalogImpl);

        Builder catalogURI(String catalogURI);

        Builder warehouseLocation(String warehouseLocation);

        Builder fileIOImpl(FILEIO_IMPL fileIOImpl);

        Builder s3EndpointOverride(String s3EndpointOverride);

        Builder s3AccessKeyId(String s3AccessKeyId);

        Builder s3SecretAccessKey(String s3SecretAccessKey);

        Builder s3Region(String s3Region);

        IcebergInstructions build();
    }

    @Check
    final void checkCatalogURI() {
        if (catalogURI() == null || catalogURI().isEmpty()) {
            throw new IllegalArgumentException("catalogURI must be provided");
        }
    }

    @Check
    final void checkWarehouseLocation() {
        if (warehouseLocation() == null || warehouseLocation().isEmpty()) {
            throw new IllegalArgumentException("warehouseLocation must be provided");
        }
    }

    @Check
    final void checkS3Fields() {
        if (fileIOImpl() == FILEIO_IMPL.S3) {
            if (s3AccessKeyId() == null || s3AccessKeyId().isEmpty()) {
                throw new IllegalArgumentException("When using S3 FileIO, s3AccessKeyId must be provided");
            }
            if (s3SecretAccessKey() == null || s3SecretAccessKey().isEmpty()) {
                throw new IllegalArgumentException("When using S3 FileIO, s3SecretAccessKey must be provided");
            }
            if (s3Region() == null || s3Region().isEmpty()) {
                throw new IllegalArgumentException("When using S3 FileIO, s3Region must be provided");
            }
        }
    }

}
