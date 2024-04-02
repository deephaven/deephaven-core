//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;

import java.nio.ByteBuffer;

/**
 * An {@link ObjectType object type plugin} named {@value NAME} that echos back the received contents back to the
 * client. When {@link #compatibleClientConnection(Object, MessageStream)} is called, an empty payload is sent to the
 * client.
 *
 * <p>
 * The only object that is compatible with this plugin is {@link #INSTANCE}.
 */
@AutoService(ObjectType.class)
public final class EchoObjectType extends ObjectTypeBase {
    public static final String NAME = "Echo";

    public static final Object INSTANCE = new Object();

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean isType(Object object) {
        return object == INSTANCE;
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException {
        // The contract right now means we need to return a message right away.
        connection.onData(ByteBuffer.allocate(0));
        return connection;
    }
}
