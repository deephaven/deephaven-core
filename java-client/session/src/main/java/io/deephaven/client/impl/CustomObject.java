/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.ServerObject.Bidirectional;
import io.deephaven.client.impl.ServerObject.Fetchable;

/**
 * A custom server object that may be {@link Fetchable} or {@link Bidirectional}. The client is responsible for
 * implementing the necessarily retrieval logic according to the specific {@link #type()}.
 *
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/4486">deephaven-core#4486</a>
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/4487">deephaven-core#4487</a>
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/4488">deephaven-core#4488</a>
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
     * The object type.
     *
     * @return the object type
     */
    public String type() {
        return exportId().type().orElseThrow(IllegalStateException::new);
    }
}
