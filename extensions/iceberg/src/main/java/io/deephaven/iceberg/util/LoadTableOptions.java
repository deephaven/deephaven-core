//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;
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
     * The name mapping provider, a fallback for resolving fields from data files that are written without
     * {@link Types.NestedField#fieldId() field ids}. By default, is {@link NameMappingProvider#fromTable()}.
     */
    @Value.Default
    public NameMappingProvider nameMapping() {
        return NameMappingProvider.fromTable();
    }

    public interface Builder {
        default Builder id(String id) {
            return id(TableIdentifier.parse(id));
        }

        Builder id(TableIdentifier id);

        default Builder resolver(Resolver resolver) {
            return resolver(ResolverProvider.of(resolver));
        }

        Builder resolver(ResolverProvider resolver);

        default Builder nameMapping(NameMapping nameMapping) {
            return nameMapping(NameMappingProvider.of(nameMapping));
        }

        Builder nameMapping(NameMappingProvider nameMapping);

        LoadTableOptions build();
    }
}
