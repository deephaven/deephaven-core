/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.qst.table.TableSpec;

import java.util.List;
import java.util.concurrent.Future;

public interface TableServiceAsync {

    interface TableHandleAsync extends Future<TableHandle> {

    }

    TableHandleAsync executeAsync(TableSpec tableSpec);

    List<? extends TableHandleAsync> executeAsync(List<TableSpec> tableSpecs);
}
