//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import java.util.function.Function;

@FunctionalInterface
public interface TableSpecFunction extends Function<TableSpec, TableSpec> {

}
