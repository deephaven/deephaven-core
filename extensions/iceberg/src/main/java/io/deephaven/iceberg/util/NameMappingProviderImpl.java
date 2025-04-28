//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;

abstract class NameMappingProviderImpl implements NameMappingProvider {

    abstract NameMapping create(Table table);
}
