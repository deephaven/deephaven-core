//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.apache.iceberg.catalog.TableIdentifier;
import org.immutables.value.Value;

/**
 * The options available for {@link IcebergCatalogAdapter#loadTable(LoadTableOptions) loadTable}.
 */
@Value.Immutable
@BuildableStyle
public abstract class LoadTableOptions {

    public static Builder builder() {
        return ImmutableLoadTableOptions.builder();
    }

    /**
     * The table identifier.
     */
    public abstract TableIdentifier id();

    /**
     * The resolver provider. By default, is {@link ResolverProvider#infer()}.
     */
    @Value.Default
    public ResolverProvider resolver() {
        return ResolverProvider.infer();
    }

    /**
     * The name mapping provider. By default, is {@link NameMappingProvider#fromTable()}.
     */
    @Value.Default
    public NameMappingProvider nameMapping() {
        return NameMappingProvider.fromTable();
    }

    public interface Builder {
        Builder id(TableIdentifier id);

        Builder resolver(ResolverProvider resolver);

        Builder nameMapping(NameMappingProvider nameMapping);

        LoadTableOptions build();
    }
}
