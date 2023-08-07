/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.ServerObject.Bidirectional;
import io.deephaven.client.impl.ServerObject.Fetchable;

/**
 * A custom server object that may be {@link Fetchable} or {@link Bidirectional}. The client is responsible for
 * implementing the necessarily retrieval logic according to the specific {@link #type()}.
 */
public final class CustomObject extends ServerObjectBase
        implements ServerObject, Fetchable, Bidirectional {

    CustomObject(Session session, ExportId exportId) {
        super(session, exportId);
        if (!exportId.type().isPresent()) {
            throw new IllegalArgumentException("Expected type to be present, is not");
        }
    }

    /**
     * The plugin type.
     *
     * @return the plugin type
     */
    public String type() {
        return exportId().type().orElseThrow(IllegalStateException::new);
    }

    @Override
    public <R> R walk(Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
