//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.Fetchable;
import io.deephaven.client.impl.ObjectService.MessageStream;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A custom server object that may be {@link Fetchable} or {@link Bidirectional}. The client is responsible for
 * implementing the necessarily retrieval logic according to the specific {@link #type()}.
 *
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/4486">deephaven-core#4486</a>
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/4487">deephaven-core#4487</a>
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/4488">deephaven-core#4488</a>
 */
public final class CustomObject extends ServerObjectBase
        implements Fetchable, Bidirectional {

    private static final Set<String> KNOWN_TYPES = Collections.singleton(TableObject.TYPE);

    CustomObject(Session session, ExportId exportId) {
        super(session, exportId);
        if (!exportId.type().isPresent()) {
            throw new IllegalArgumentException("Expected type to be present, is not");
        }
        final String type = exportId.type().get();
        if (KNOWN_TYPES.contains(type)) {
            throw new IllegalArgumentException(
                    String.format("Can't construct a CustomObject with a well-known type '%s'", type));
        }
    }

    @Override
    public String type() {
        return exportId().type().orElseThrow(IllegalStateException::new);
    }

    @Override
    public CompletableFuture<ServerData> fetch() {
        return session.fetch(this);
    }

    @Override
    public MessageStream<ClientData> connect(MessageStream<ServerData> receiveStream) {
        return session.connect(this, receiveStream);
    }
}
