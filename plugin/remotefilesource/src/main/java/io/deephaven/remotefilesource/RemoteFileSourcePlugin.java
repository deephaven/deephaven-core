//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.ObjectCommunicationException;

/**
 * ObjectType plugin for remote file sources. This plugin is registered via @AutoService and handles creation of
 * RemoteFileSourceMessageStream connections for bidirectional communication with clients.
 * <p>
 * The only object that is compatible with this plugin is {@link #INSTANCE}. When a connection is established, a
 * RemoteFileSourceMessageStream is created to handle bidirectional message passing between client and server.
 * <p>
 * A RemoteFileSourceMessageStream serves resources for a script evaluation only once its client has declared them; see
 * {@link io.deephaven.engine.util.remotefilesource.RemoteFileSourceClassLoader} for how a declaration is claimed by the
 * run it was made for.
 * <p>
 * Clients obtain {@link #INSTANCE} by running a script that assigns it to a temporary variable, so only callers
 * permitted to run code on the worker can connect.
 */
@AutoService(ObjectType.class)
public class RemoteFileSourcePlugin extends ObjectTypeBase {
    public static final String NAME = "DeephavenRemoteFileSourcePlugin";

    /**
     * The stateless object clients connect to this plugin through. Clients reference it by its fully qualified name in
     * a script, so moving or renaming it breaks them.
     */
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
        if (!isType(object)) {
            throw new ObjectCommunicationException(
                    "Expected RemoteFileSourcePlugin.INSTANCE, got " + object.getClass());
        }

        // Return a new bidirectional message stream for this connection
        return new RemoteFileSourceMessageStream(connection);
    }
}

