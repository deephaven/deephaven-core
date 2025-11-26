package io.deephaven.remotefilesource;

import com.google.auto.service.AutoService;
import com.google.protobuf.InvalidProtocolBufferException;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.proto.backplane.grpc.RemoteFileSourcePluginFetchRequest;

import java.nio.ByteBuffer;

@AutoService(ObjectType.class)
public class RemoteFileSourceServicePlugin extends ObjectTypeBase {
    public RemoteFileSourceServicePlugin() {}

    @Override
    public String name() {
        return "RemoteFileSourceService";
    }

    @Override
    public boolean isType(Object object) {
        return object instanceof RemoteFileSourceServicePlugin;
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection) throws ObjectCommunicationException {
        connection.onData(ByteBuffer.allocate(0));
        return new RemoteFileSourceMessageStream(connection);
    }

    /**
     * A message stream for the RemoteFileSourceService.
     */
    private class RemoteFileSourceMessageStream implements MessageStream {
        private final MessageStream connection;

        public RemoteFileSourceMessageStream(final MessageStream connection) {
            this.connection = connection;
        }

        @Override
        public void onData(ByteBuffer payload, Object... references) throws ObjectCommunicationException {
            final RemoteFileSourcePluginFetchRequest request;

            try {
                request = RemoteFileSourcePluginFetchRequest.parseFrom(payload);
            } catch (InvalidProtocolBufferException e) {
                // There is no identifier here, so we cannot properly return an error that is bound to the request.
                // Instead, we throw an Exception causing the server to close the entire MessageStream and
                // propagate a general error to the client.
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onClose() {

        }
    }
}
