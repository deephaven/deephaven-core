//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;

/**
 * A {@value TYPE} object.
 */
public final class TableObject extends ServerObjectBase {

    public static final String TYPE = "Table";

    TableObject(Session session, ExportId exportId) {
        super(session, exportId);
        checkType(TYPE, exportId);
    }

    /**
     * Creates a table handle.
     *
     * <p>
     * Note: the table handle lifecycle is managed separately from {@code this}; the caller is still responsible for
     * {@link #close() closing} {@code this}.
     *
     * @return the table handle.
     * @see Session#execute(TableSpec)
     */
    public TableHandle executeTable() throws TableHandleException, InterruptedException {
        return session.execute(tableSpec());
    }

    public TicketTable tableSpec() {
        return TableSpec.ticket(exportId().ticket());
    }
}
