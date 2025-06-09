//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;

import java.util.Objects;

final class NameMappingProviderImpl implements NameMappingProvider.Visitor<NameMapping> {

    public static NameMapping of(NameMappingProvider provider, Table table) {
        return provider.walk(new NameMappingProviderImpl(table));
    }

    private final Table table;

    NameMappingProviderImpl(Table table) {
        this.table = Objects.requireNonNull(table);
    }

    @Override
    public NameMapping visit(NameMappingProvider.TableNameMapping tableNameMapping) {
        return NameMappingUtil.readNameMappingDefault(table).orElse(NameMapping.empty());
    }

    @Override
    public NameMapping visit(NameMappingProvider.EmptyNameMapping emptyNameMapping) {
        return NameMapping.empty();
    }

    @Override
    public NameMapping visit(NameMappingProvider.DirectNameMapping directNameMapping) {
        return directNameMapping.nameMapping();
    }
}
