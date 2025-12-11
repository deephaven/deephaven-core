//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.auto.service.AutoService;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.PluginMarker;

import java.nio.ByteBuffer;

/**
 * ObjectType plugin for RemoteFileSource. This plugin is registered via @AutoService
 * and handles creation of RemoteFileSourceMessageStream connections.
 *
 * This plugin uses a PluginMarker with a type field instead of instanceof checks,
 * allowing it to work across language boundaries (Java/Python). The Flight command
 * creates a PluginMarker with type="RemoteFileSource" which this plugin recognizes.
 *
 * Each RemoteFileSourceMessageStream instance registers itself as a provider with the
 * ClassLoader when created and unregisters when closed. The ClassLoader checks isActive()
 * on each registered provider to find the currently active one.
 */
@AutoService(ObjectType.class)
public class RemoteFileSourcePlugin extends ObjectTypeBase {
    private static final Logger log = LoggerFactory.getLogger(RemoteFileSourcePlugin.class);

    @Override
    public String name() {
        return "DeephavenGroovyRemoteFileSourcePlugin";
    }

    @Override
    public boolean isType(Object object) {
        // We need to check the pluginType
        if (object instanceof PluginMarker) {
            PluginMarker marker = (PluginMarker) object;
            return name().equals(marker.getPluginType());
        }
        return false;
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException {
        if (!isType(object)) {
            throw new ObjectCommunicationException("Expected RemoteFileSource marker object, got " + object.getClass());
        }

        connection.onData(ByteBuffer.allocate(0));

        // Create and return a new message stream for this connection
        // All the logic is in the static RemoteFileSourceMessageStream class
        return new RemoteFileSourceMessageStream(connection);
    }
}

