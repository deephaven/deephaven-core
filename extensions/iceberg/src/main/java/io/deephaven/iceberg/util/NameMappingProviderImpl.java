//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;

import java.util.Objects;

interface NameMappingProviderImpl extends NameMappingProvider {

    NameMapping create(Table table);

    enum Empty implements NameMappingProviderImpl {
        EMPTY;

        @Override
        public NameMapping create(Table table) {
            return NameMapping.empty();
        }
    }

    enum FromTable implements NameMappingProviderImpl {
        FROM_TABLE;

        @Override
        public NameMapping create(Table table) {
            return NameMappingUtil.readNameMappingDefault(table).orElse(NameMapping.empty());
        }
    }

    final class Explicit implements NameMappingProviderImpl {
        private final NameMapping impl;

        public Explicit(NameMapping impl) {
            this.impl = Objects.requireNonNull(impl);
        }

        @Override
        public NameMapping create(Table table) {
            return impl;
        }
    }
}
