/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.ServerObject.Fetchable;

/**
 * A {@value TYPE} object.
 *
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/4486">deephaven-core#4486</a>
 */
public final class PartitionedTableObject extends ServerObjectBase
        implements ServerObject, Fetchable {

    public static final String TYPE = "PartitionedTable";

    PartitionedTableObject(Session session, ExportId exportId) {
        super(session, exportId);
        checkType(TYPE, exportId);
    }

    @Override
    public <R> R walk(Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
