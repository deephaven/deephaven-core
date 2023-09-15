/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

/**
 * A server object that is <b>not</b> {@link io.deephaven.client.impl.ServerObject.Fetchable} nor
 * {@link io.deephaven.client.impl.ServerObject.Bidirectional}; the server does not have a registration for the
 * underlying object.
 */
public final class UnknownObject extends ServerObjectBase implements ServerObject {

    UnknownObject(Session session, ExportId exportId) {
        super(session, exportId);
        if (exportId.type().isPresent()) {
            throw new IllegalArgumentException("Expected type to not be present, is present");
        }
    }
}
