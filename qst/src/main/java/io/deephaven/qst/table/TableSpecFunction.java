package io.deephaven.qst.table;

import java.util.function.Function;

@FunctionalInterface
public interface TableSpecFunction extends Function<TableSpec, TableSpec> {

}
