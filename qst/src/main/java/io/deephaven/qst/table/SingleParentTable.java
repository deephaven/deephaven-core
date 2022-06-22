/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

public interface SingleParentTable extends TableSpec {

    TableSpec parent();
}
