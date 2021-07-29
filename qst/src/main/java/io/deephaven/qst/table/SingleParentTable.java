package io.deephaven.qst.table;

public interface SingleParentTable extends TableSpec {

    TableSpec parent();
}
