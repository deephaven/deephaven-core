//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.PluginMarker;

import java.nio.ByteBuffer;

/**
 * ObjectType plugin for remote file sources. This plugin is registered via @AutoService
 * and handles creation of RemoteFileSourceMessageStream connections for bidirectional
 * communication with clients.
 * <p>
 * This plugin recognizes PluginMarker objects whose pluginName matches this plugin's name.
 * When a connection is established, a RemoteFileSourceMessageStream is created to handle
 * bidirectional message passing between client and server.
 * <p>
 * Each RemoteFileSourceMessageStream instance registers itself as a provider with the
 * RemoteFileSourceClassLoader when created and unregisters when closed. The RemoteFileSourceClassLoader
 * uses isActive() to determine which registered provider should handle resource requests.
 */
@AutoService(ObjectType.class)
public class RemoteFileSourcePlugin extends ObjectTypeBase {

    @Override
    public String name() {
        return "DeephavenRemoteFileSourcePlugin";
    }

    @Override
    public boolean isType(Object object) {
        if (object instanceof PluginMarker) {
            PluginMarker marker = (PluginMarker) object;
            return name().equals(marker.getPluginName());
        }
        return false;
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException {
        if (!isType(object)) {
            throw new ObjectCommunicationException("Expected RemoteFileSource marker object, got " + object.getClass());
        }

        // Send initial empty message to client as required by the ObjectType contract
        connection.onData(ByteBuffer.allocate(0));

        // Return a new bidirectional message stream for this connection
        return new RemoteFileSourceMessageStream(connection);
    }
}

