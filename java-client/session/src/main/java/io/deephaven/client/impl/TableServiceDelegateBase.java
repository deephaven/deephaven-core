/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.qst.table.TableSpec;

import java.util.List;

abstract class TableServiceDelegateBase extends TableHandleManagerDelegate implements TableService {

    @Override
    protected abstract TableService delegate();

    // ---------------------------------------------------

    @Override
    public TableHandleManager batch() {
        return delegate().batch();
    }

    @Override
    public TableHandleManager batch(boolean mixinStacktraces) {
        return new TableHandleManagerDelegate() {
            @Override
            protected TableHandleManager delegate() {
                return null;
            }
        };
    }

    @Override
    public TableHandleManager serial() {
        return delegate().serial();
    }

    // ---------------------------------------------------

    @Override
    public TableHandleFuture executeAsync(TableSpec table) {
        return delegate().executeAsync(table);
    }

    @Override
    public List<? extends TableHandleFuture> executeAsync(Iterable<? extends TableSpec> tables) {
        return delegate().executeAsync(tables);
    }
}
