//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;

public interface NameMappingProvider {

    static NameMappingProvider of(NameMapping nameMapping) {
        return new NameMappingProviderImpl() {
            @Override
            NameMapping create(Table table) {
                return nameMapping;
            }
        };
    }

    static NameMappingProvider empty() {
        return of(NameMapping.empty());
    }

    static NameMappingProvider fromTable() {
        return new NameMappingProviderImpl() {
            @Override
            NameMapping create(Table table) {
                return NameMappingUtil.readNameMappingDefault(table).orElse(null);
            }
        };
    }
}
