//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.Fetchable;

/**
 * A server object that is <b>neither</b> {@link Fetchable} nor {@link Bidirectional}; the server does not have an
 * object type plugin registered for the referenced object. See the server side class
 * {@code io.deephaven.plugin.type.ObjectType} for more details.
 */
public final class UnknownObject extends ServerObjectBase {

    UnknownObject(Session session, ExportId exportId) {
        super(session, exportId);
        if (exportId.type().isPresent()) {
            throw new IllegalArgumentException("Expected type to not be present, is present");
        }
    }
}
