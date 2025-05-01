//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;

abstract class ResolverProviderImpl implements ResolverProvider {

    abstract Resolver resolver(Table table) throws TypeInference.UnsupportedType;
}
