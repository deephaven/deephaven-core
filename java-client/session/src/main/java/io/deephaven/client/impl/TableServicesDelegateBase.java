/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.qst.table.TableSpec;

import java.util.List;

abstract class TableServicesDelegateBase extends TableHandleManagerDelegate implements TableServices {

    @Override
    protected abstract TableServices delegate();

    // ---------------------------------------------------

    @Override
    public Export export(TableSpec table) {
        return delegate().export(table);
    }

    @Override
    public List<Export> export(ExportsRequest request) {
        return delegate().export(request);
    }

    // ---------------------------------------------------

    @Override
    public TableHandleManager batch() {
        return delegate().batch();
    }

    @Override
    public TableHandleManager batch(boolean mixinStacktraces) {
        return delegate().batch(mixinStacktraces);
    }

    @Override
    public TableHandleManager serial() {
        return delegate().serial();
    }

    // ---------------------------------------------------

    @Override
    public TableHandleAsync executeAsync(TableSpec tableSpec) {
        return delegate().executeAsync(tableSpec);
    }

    @Override
    public List<? extends TableHandleAsync> executeAsync(List<TableSpec> tableSpecs) {
        return delegate().executeAsync(tableSpecs);
    }
}
