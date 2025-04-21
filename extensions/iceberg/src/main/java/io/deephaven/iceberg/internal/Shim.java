//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.iceberg.util.Resolver;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.iceberg.mapping.NameMapping;

@InternalUseOnly
public class Shim {

    public static ParquetColumnResolver.Factory factory(Resolver resolver, NameMapping nameMapping) {
        return new ResolverFactory(resolver, nameMapping);
    }
}
