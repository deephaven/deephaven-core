package io.deephaven.client.impl;

import io.deephaven.client.impl.ServerObject.Fetchable;

/**
 * A {@value TYPE} object.
 *
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/4488">deephaven-core#4488</a>
 */
public final class FigureObject extends ServerObjectBase implements ServerObject, Fetchable {

    public static final String TYPE = "Figure";

    FigureObject(Session session, ExportId exportId) {
        super(session, exportId);
        checkType(TYPE, exportId);
    }

    @Override
    public <R> R walk(Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
