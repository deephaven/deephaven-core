//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.protobuf.InvalidProtocolBufferException;
import io.deephaven.engine.util.RemoteFileSourceClassLoader;
import io.deephaven.engine.util.RemoteFileSourceProvider;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceClientRequest;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaRequest;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaResponse;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceServerRequest;
import io.deephaven.proto.backplane.grpc.SetExecutionContextResponse;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Message stream implementation for RemoteFileSource bidirectional communication.
 * Each instance represents a file source provider for one client connection and implements
 * RemoteFileSourceProvider so it can be registered with the RemoteFileSourceClassLoader.
 * Only one MessageStream can be "active" at a time (determined by the execution context).
 * The RemoteFileSourceClassLoader checks isActive() on each registered provider to find the active one.
 */
public class RemoteFileSourceMessageStream implements ObjectType.MessageStream, RemoteFileSourceProvider {
    private static final Logger log = LoggerFactory.getLogger(RemoteFileSourceMessageStream.class);

    /**
     * The current execution context containing the active message stream and configuration.
     * Null when no execution context is active.
     * Used by this class's isActive() and canSourceResource() methods to determine if this
     * provider should handle resource requests from RemoteFileSourceClassLoader.
     */
    private static volatile RemoteFileSourceExecutionContext executionContext;


    private final ObjectType.MessageStream connection;
    private final Map<String, CompletableFuture<byte[]>> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Creates a new RemoteFileSourceMessageStream for the given connection.
     * Automatically registers this instance as a provider with the RemoteFileSourceClassLoader.
     *
     * @param connection the message stream connection to the client
     */
    public RemoteFileSourceMessageStream(final ObjectType.MessageStream connection) {
        this.connection = connection;
        // Register this instance as a provider with the RemoteFileSourceClassLoader
        registerWithClassLoader();
    }

    /**
     * Determines if this provider can source the specified resource.
     * Only returns true if this message stream is active, the resource is a .groovy file,
     * and the resource path matches one of the configured resource paths.
     *
     * @param resourcePath the path of the resource to check
     * @return true if this provider can source the resource, false otherwise
     */
    @Override
    public boolean canSourceResource(String resourcePath) {
        // Only active if this instance is the currently active message stream
        if (!isActive()) {
            return false;
        }

        // Only handle .groovy source files, not compiled .class files
        if (!resourcePath.endsWith(".groovy")) {
            return false;
        }

        RemoteFileSourceExecutionContext context = executionContext;

        List<String> resourcePaths = context.getResourcePaths();
        if (resourcePaths.isEmpty()) {
            return false;
        }

        for (String contextResourcePath : resourcePaths) {
            if (resourcePath.equals(contextResourcePath)) {
                log.info().append("Can source: ").append(resourcePath).endl();
                return true;
            }
        }

        return false;
    }

    /**
     * Requests a resource from the remote client.
     * Sends a request to the client and returns a future that will be completed when the client responds.
     * Only services requests if this message stream is active.
     *
     * @param resourcePath the name of the resource to request
     * @return a CompletableFuture that will contain the resource bytes when available, or null if inactive
     */
    @Override
    public CompletableFuture<byte[]> requestResource(String resourcePath) {
        // Only service requests if this instance is active
        if (!isActive()) {
            log.warn().append("Request for resource ").append(resourcePath)
                    .append(" on inactive message stream").endl();
            return CompletableFuture.completedFuture(null);
        }

        log.info().append("Requesting resource: ").append(resourcePath).endl();

        String requestId = UUID.randomUUID().toString();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pendingRequests.put(requestId, future);

        try {
            // Build RemoteFileSourceMetaRequest proto
            RemoteFileSourceMetaRequest metaRequest =
                    RemoteFileSourceMetaRequest.newBuilder()
                    .setResourceName(resourcePath)
                    .build();

            // Wrap in RemoteFileSourceServerRequest (server→client)
            RemoteFileSourceServerRequest message =
                    RemoteFileSourceServerRequest.newBuilder()
                    .setRequestId(requestId)
                    .setMetaRequest(metaRequest)
                    .build();

            ByteBuffer buffer = ByteBuffer.wrap(message.toByteArray());

            log.info().append("Sending resource request for: ").append(resourcePath)
                    .append(" with requestId: ").append(requestId).endl();

            connection.onData(buffer);
        } catch (ObjectCommunicationException e) {
            future.completeExceptionally(e);
            pendingRequests.remove(requestId);
        }

        return future;
    }

    /**
     * Checks if this message stream is currently active.
     * A message stream is active when the execution context is set and this instance is the active stream.
     *
     * @return true if this message stream is active, false otherwise
     */
    @Override
    public boolean isActive() {
        RemoteFileSourceExecutionContext context = executionContext;
        return context != null && context.getActiveMessageStream() == this;
    }

    /**
     * Sets the execution context with the active message stream and resource paths.
     * This should be called when a script execution begins.
     *
     * @param messageStream the message stream to set as active
     * @param resourcePaths list of resource paths to resolve from remote source
     * @throws IllegalArgumentException if messageStream is null
     */
    public static void setExecutionContext(RemoteFileSourceMessageStream messageStream, List<String> resourcePaths) {
        if (messageStream == null) {
            throw new IllegalArgumentException("messageStream must not be null");
        }

        executionContext = new RemoteFileSourceExecutionContext(messageStream, resourcePaths);
        log.info().append("Set execution context with ")
                .append(executionContext.getResourcePaths().size()).append(" resource paths").endl();
    }

    /**
     * Clears the execution context.
     */
    public static void clearExecutionContext() {
        if (executionContext != null) {
            executionContext = null;
            log.info().append("Cleared execution context").endl();
        }
    }

    /**
     * Gets the current execution context.
     *
     * @return the execution context
     */
    public static RemoteFileSourceExecutionContext getExecutionContext() {
        return executionContext;
    }

    /**
     * Handles incoming data from the client.
     * Parses RemoteFileSourceClientRequest messages and processes meta responses
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
            RemoteFileSourceClientRequest message = RemoteFileSourceClientRequest.parseFrom(bytes);

            if (message.hasMetaResponse()) {
                handleMetaResponse(message.getRequestId(), message.getMetaResponse());
            } else if (message.hasSetExecutionContext()) {
                handleSetExecutionContext(message.getRequestId(), message.getSetExecutionContext().getResourcePathsList());
            } else {
                log.warn().append("Received unknown message type from client").endl();
            }
        } catch (InvalidProtocolBufferException e) {
            log.error().append("Failed to parse RemoteFileSourceClientRequest: ").append(e).endl();
            throw new ObjectCommunicationException("Failed to parse message", e);
        }
    }

    /**
     * Handles a meta response from the client containing requested resource content.
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

        if (!response.getError().isEmpty()) {
            log.warn().append("Error in response: ").append(response.getError()).endl();
        }

        future.complete(content);
    }

    /**
     * Handles a request from the client to set the execution context.
     *
     * @param requestId the request ID
     * @param resourcePaths the list of resource paths to resolve from remote source
     */
    private void handleSetExecutionContext(String requestId, List<String> resourcePaths) {
        setExecutionContext(this, resourcePaths);
        log.info().append("Client set execution context for this message stream with ")
                .append(resourcePaths.size()).append(" resource paths").endl();

        sendExecutionContextAcknowledgment(requestId);
    }

    /**
     * Sends an acknowledgment to the client that the execution context was successfully set.
     *
     * @param requestId the request ID to acknowledge
     */
    private void sendExecutionContextAcknowledgment(String requestId) {
        SetExecutionContextResponse response = SetExecutionContextResponse.newBuilder()
                .setSuccess(true)
                .build();

        RemoteFileSourceServerRequest serverRequest = RemoteFileSourceServerRequest.newBuilder()
                .setRequestId(requestId)
                .setSetExecutionContextResponse(response)
                .build();

        try {
            connection.onData(ByteBuffer.wrap(serverRequest.toByteArray()));
        } catch (ObjectCommunicationException e) {
            log.error().append("Failed to send execution context acknowledgment: ").append(e).endl();
        }
    }

    /**
     * Handles cleanup when the message stream is closed.
     * Unregisters this provider from the RemoteFileSourceClassLoader, clears the execution context if this was active,
     * and cancels all pending resource requests.
     */
    @Override
    public void onClose() {
        // Unregister this provider from the RemoteFileSourceClassLoader
        unregisterFromClassLoader();

        // Clear execution context if this was the active stream
        if (isActive()) {
            clearExecutionContext();
        }

        // Cancel all pending requests
        pendingRequests.values().forEach(future -> future.cancel(true));
        pendingRequests.clear();
    }

    /**
     * Register this message stream instance as a provider with the RemoteFileSourceClassLoader.
     */
    private void registerWithClassLoader() {
        RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();

        if (classLoader != null) {
            classLoader.registerProvider(this);
            log.info().append("Registered RemoteFileSourceMessageStream provider with RemoteFileSourceClassLoader").endl();
        } else {
            log.warn().append("RemoteFileSourceClassLoader not available").endl();
        }
    }

    /**
     * Unregister this message stream instance from the RemoteFileSourceClassLoader.
     */
    private void unregisterFromClassLoader() {
        RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();

        if (classLoader != null) {
            classLoader.unregisterProvider(this);
            log.info().append("Unregistered RemoteFileSourceMessageStream provider from RemoteFileSourceClassLoader").endl();
        }
    }


    /**
     * Encapsulates the execution context for remote file source operations.
     * This includes the currently active message stream and the resource paths
     * that should be resolved from the remote source.
     * This class is immutable - a new instance is created each time the context changes.
     */
    public static class RemoteFileSourceExecutionContext {
        private final RemoteFileSourceMessageStream activeMessageStream;
        private final List<String> resourcePaths;

        /**
         * Creates a new execution context.
         *
         * @param activeMessageStream the active message stream
         * @param resourcePaths list of resource paths to resolve from remote source
         */
        public RemoteFileSourceExecutionContext(RemoteFileSourceMessageStream activeMessageStream,
                List<String> resourcePaths) {
            this.activeMessageStream = activeMessageStream;
            this.resourcePaths = resourcePaths != null ? resourcePaths : Collections.emptyList();
        }

        /**
         * Gets the currently active message stream.
         *
         * @return the active message stream
         */
        public RemoteFileSourceMessageStream getActiveMessageStream() {
            return activeMessageStream;
        }

        /**
         * Gets the resource paths that should be resolved from the remote source.
         *
         * @return a copy of the list of resource paths
         */
        public List<String> getResourcePaths() {
            return new ArrayList<>(resourcePaths);
        }
    }
}

