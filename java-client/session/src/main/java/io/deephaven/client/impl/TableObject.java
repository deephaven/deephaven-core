/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;

/**
 * A {@value TYPE} object.
 */
public final class TableObject extends ServerObjectBase implements ServerObject {

    public static final String TYPE = "Table";

    TableObject(Session session, ExportId exportId) {
        super(session, exportId);
        checkType(TYPE, exportId);
    }

    public TableHandle executeTable() throws TableHandleException, InterruptedException {
        return session.execute(tableSpec());
    }

    public TicketTable tableSpec() {
        return TableSpec.ticket(exportId().ticket());
    }

    @Override
    public <R> R walk(Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
