/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.qst.table.TableSpec;

public abstract class SessionBase extends TableHandleManagerDelegate implements Session {

    @Override
    public final Export export(TableSpec table) {
        return export(ExportsRequest.logging(table)).get(0);
    }
}
