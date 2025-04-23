//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.apache.iceberg.catalog.TableIdentifier;
import org.immutables.value.Value;

@Value.Immutable
@BuildableStyle
public abstract class LoadTableOptions {

    public static Builder builder() {
        return ImmutableLoadTableOptions.builder();
    }

    public abstract TableIdentifier id();

    @Value.Default
    public ResolverProvider resolver() {
        return ResolverProvider.infer();
    }

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
