//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.protobuf.InvalidProtocolBufferException;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.util.RemoteFileSourceClassLoader;
import io.deephaven.engine.util.RemoteFileSourceProvider;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceClientMessage;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaRequest;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaResponse;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceServerMessage;
import io.deephaven.proto.backplane.grpc.SetExecutionContextRequest;
import io.deephaven.proto.backplane.grpc.SetExecutionContextResponse;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Message stream implementation for RemoteFileSource bidirectional communication. Each instance represents a file
 * source provider for one client connection and implements RemoteFileSourceProvider so the RemoteFileSourceClassLoader
 * can fetch resources through it.
 *
 * <p>
 * The client declares the resources it will serve before each script run; see {@link RemoteFileSourceClassLoader} for
 * how a declaration is claimed by the run it was made for.
 */
public class RemoteFileSourceMessageStream implements ObjectType.MessageStream, RemoteFileSourceProvider {
    private static final Logger log = LoggerFactory.getLogger(RemoteFileSourceMessageStream.class);

    private final ObjectType.MessageStream connection;
    private final Map<String, CompletableFuture<byte[]>> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Set once the client's stream has closed, after which no further messages may be sent on it.
     */
    private volatile boolean closed;

    /**
     * Creates a new RemoteFileSourceMessageStream for the given connection.
     *
     * @param connection the message stream connection to the client
     * @throws ObjectCommunicationException if the initial message cannot be sent to the client
     */
    public RemoteFileSourceMessageStream(final ObjectType.MessageStream connection)
            throws ObjectCommunicationException {
        this.connection = connection;
        // Send initial empty message to client as required by the ObjectType contract
        connection.onData(ByteBuffer.allocate(0));
    }

    /**
     * Requests a resource from the remote client. Sends a request to the client and returns a future that will be
     * completed when the client responds. Only services requests if this message stream is active.
     *
     * @param resourceName the name of the resource to request
     * @return a CompletableFuture that completes with the resource bytes, or completes with null if the client could
     *         not find the resource. The future completes exceptionally if this message stream is not active, if the
     *         request could not be sent, or if the client reported an error.
     */
    @Override
    public CompletableFuture<byte[]> requestResource(String resourceName) {
        log.info().append("Requesting resource: ").append(resourceName).endl();

        String requestId = UUID.randomUUID().toString();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pendingRequests.put(requestId, future);
        // Drop the entry on every completion path, not just when a response arrives. The caller applies its own
        // timeout to this future, and a request whose response never arrives would otherwise be retained until the
        // stream closes.
        future.whenComplete((result, error) -> pendingRequests.remove(requestId));

        // Checked after publishing the future, so that a close racing this call either cancels the future itself or
        // is seen here. Resources resolved before the client disconnected can still be fetched afterwards, and the
        // caller would otherwise wait out its timeout for a response that cannot arrive.
        if (closed) {
            future.completeExceptionally(new IllegalStateException(
                    "Remote file source connection closed before requesting " + resourceName));
            return future;
        }

        try {
            // Build RemoteFileSourceMetaRequest proto
            RemoteFileSourceMetaRequest metaRequest =
                    RemoteFileSourceMetaRequest.newBuilder()
                            .setResourceName(resourceName)
                            .build();

            // Wrap in RemoteFileSourceServerMessage (server→client)
            RemoteFileSourceServerMessage message =
                    RemoteFileSourceServerMessage.newBuilder()
                            .setRequestId(requestId)
                            .setMetaRequest(metaRequest)
                            .build();

            ByteBuffer buffer = ByteBuffer.wrap(message.toByteArray());

            log.info().append("Sending resource request for: ").append(resourceName)
                    .append(" with requestId: ").append(requestId).endl();

            connection.onData(buffer);
        } catch (ObjectCommunicationException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Declares this connection's resources with the class loader, for the evaluation that begins next.
     *
     * @param resourcePaths resource paths (e.g., "package/MyScript.groovy") to resolve from this connection
     * @param dirty whether remote sources have changed and the cache should be cleared
     */
    private void declareExecutionContext(final List<String> resourcePaths, final boolean dirty) {
        RemoteFileSourceClassLoader.getInstance().declareExecutionContext(this, resourcePaths, dirty);
        log.info().append("Declared execution context with ")
                .append(resourcePaths.size()).append(" resource paths")
                .append(", isDirty: ").append(dirty).endl();
    }

    /**
     * Handles incoming data from the client. Parses RemoteFileSourceClientMessage messages and processes meta responses
     * or execution context updates from the client.
     *
     * @param payload the message payload containing the protobuf data
     * @param references optional references (not used)
     * @throws ObjectCommunicationException if the message cannot be parsed
     */
    @Override
    public void onData(ByteBuffer payload, Object... references) throws ObjectCommunicationException {
        try {
            byte[] bytes = new byte[payload.remaining()];
            payload.get(bytes);
            RemoteFileSourceClientMessage message = RemoteFileSourceClientMessage.parseFrom(bytes);

            if (message.hasMetaResponse()) {
                handleMetaResponse(message.getRequestId(), message.getMetaResponse());
            } else if (message.hasSetExecutionContext()) {
                handleSetExecutionContext(message.getRequestId(), message.getSetExecutionContext());
            } else {
                log.error().append("Received unknown message type from client").endl();
                throw new ObjectCommunicationException("Received unknown message type from client");
            }
        } catch (InvalidProtocolBufferException e) {
            log.error().append("Failed to parse RemoteFileSourceClientMessage: ").append(e).endl();
            throw new ObjectCommunicationException("Failed to parse message", e);
        }
    }

    /**
     * Handles a meta response from the client containing requested resource content. An error reported by the client
     * completes the pending request exceptionally so the reason reaches the caller; a response that did not find the
     * resource completes it with null.
     *
     * @param requestId the request ID
     * @param response the meta response from the client
     */
    private void handleMetaResponse(String requestId, RemoteFileSourceMetaResponse response) {
        CompletableFuture<byte[]> future = pendingRequests.remove(requestId);
        if (future == null) {
            log.warn().append("Received response for unknown requestId: ").append(requestId).endl();
            return;
        }

        byte[] content = response.getContent().toByteArray();

        log.info().append("Received resource response for requestId: ").append(requestId)
                .append(", found: ").append(response.getFound())
                .append(", content length: ").append(content.length).endl();

        final String error = response.getError();
        if (!error.isEmpty()) {
            log.warn().append("Error in response: ").append(error).endl();
            future.completeExceptionally(new UncheckedDeephavenException(
                    "Client reported an error sourcing remote resource: " + error));
            return;
        }

        if (!response.getFound()) {
            future.complete(null);
            return;
        }

        future.complete(content);
    }

    /**
     * Handles a request from the client to set the execution context.
     *
     * @param requestId the request ID
     * @param setExecutionContext the SetExecutionContextRequest containing resource paths and isDirty flag
     */
    private void handleSetExecutionContext(String requestId, SetExecutionContextRequest setExecutionContext)
            throws ObjectCommunicationException {
        boolean isDirty = setExecutionContext.getIsDirty();
        List<String> resourcePaths = setExecutionContext.getResourcePathsList();

        declareExecutionContext(resourcePaths, isDirty);

        sendExecutionContextAcknowledgment(requestId);
    }

    /**
     * Sends an acknowledgment to the client that the execution context was successfully set.
     *
     * @param requestId the request ID to acknowledge
     */
    private void sendExecutionContextAcknowledgment(String requestId) throws ObjectCommunicationException {
        SetExecutionContextResponse response = SetExecutionContextResponse.newBuilder().build();

        RemoteFileSourceServerMessage serverRequest = RemoteFileSourceServerMessage.newBuilder()
                .setRequestId(requestId)
                .setSetExecutionContextResponse(response)
                .build();

        // Let a send failure propagate out of onData: the declaration is already installed, and the object service
        // responds to this by closing the stream, which drops it. Swallowing it would leave the client waiting out
        // its timeout while the unacknowledged declaration stayed claimable by a later evaluation.
        connection.onData(ByteBuffer.wrap(serverRequest.toByteArray()));
    }

    /**
     * Handles cleanup when the message stream is closed. Drops any declaration this connection made or is serving, and
     * cancels all pending resource requests.
     */
    @Override
    public void onClose() {
        closed = true;
        RemoteFileSourceClassLoader.getInstance().providerClosed(this);

        // Cancel all pending requests
        pendingRequests.values().forEach(future -> future.cancel(true));
        pendingRequests.clear();
    }
}
