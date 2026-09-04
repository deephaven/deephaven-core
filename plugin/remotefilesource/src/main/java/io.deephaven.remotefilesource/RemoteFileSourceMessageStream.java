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
 * source provider for one client connection and implements RemoteFileSourceProvider so it can be registered with the
 * RemoteFileSourceClassLoader. Only one MessageStream can be "active" at a time (determined by the execution context).
 * The RemoteFileSourceClassLoader checks isActive() on each registered provider to find the active one.
 */
public class RemoteFileSourceMessageStream implements ObjectType.MessageStream, RemoteFileSourceProvider {
    private static final Logger log = LoggerFactory.getLogger(RemoteFileSourceMessageStream.class);

    /**
     * The current execution context containing the active message stream and configuration. Null when no execution
     * context is active. Read via {@link #activeContextIfOwned()} to determine if this provider should handle resource
     * requests from RemoteFileSourceClassLoader.
     */
    private static volatile RemoteFileSourceExecutionContext executionContext;


    private final ObjectType.MessageStream connection;
    private final Map<String, CompletableFuture<byte[]>> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Creates a new RemoteFileSourceMessageStream for the given connection. Automatically registers this instance as a
     * provider with the RemoteFileSourceClassLoader.
     *
     * @param connection the message stream connection to the client
     * @throws ObjectCommunicationException if the initial message cannot be sent to the client
     */
    public RemoteFileSourceMessageStream(final ObjectType.MessageStream connection)
            throws ObjectCommunicationException {
        this.connection = connection;
        // Send initial empty message to client as required by the ObjectType contract
        connection.onData(ByteBuffer.allocate(0));
        // Register this instance as a provider with the RemoteFileSourceClassLoader
        registerWithClassLoader();
    }

    /**
     * Determines if this provider can source the specified resource. Only returns true if this message stream is
     * active, the resource is a .groovy file, and the resource path matches one of the configured resource paths.
     *
     * @param resourceName the name of the resource to check
     * @return true if this provider can source the resource, false otherwise
     */
    @Override
    public boolean canSourceResource(String resourceName) {
        final RemoteFileSourceExecutionContext context = activeContextIfOwned();
        if (context == null) {
            return false;
        }

        // Only handle .groovy source files, not compiled .class files
        if (!resourceName.endsWith(".groovy")) {
            return false;
        }

        for (String contextResourcePath : context.getResourcePaths()) {
            if (resourceName.equals(contextResourcePath)) {
                log.debug().append("Can source: ").append(resourceName).endl();
                return true;
            }
        }

        return false;
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
        // Only service requests if this instance is active
        if (!isActive()) {
            log.warn().append("Request for resource ").append(resourceName)
                    .append(" on inactive message stream").endl();
            return CompletableFuture.failedFuture(new IllegalStateException("Inactive message stream"));
        }

        log.info().append("Requesting resource: ").append(resourceName).endl();

        String requestId = UUID.randomUUID().toString();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pendingRequests.put(requestId, future);
        // Drop the entry on every completion path, not just when a response arrives. The caller applies its own
        // timeout to this future, and a request whose response never arrives would otherwise be retained until the
        // stream closes.
        future.whenComplete((result, error) -> pendingRequests.remove(requestId));

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
     * Checks if this message stream is currently active. A message stream is active when the execution context is set
     * and this instance is the active stream.
     *
     * @return true if this message stream is active, false otherwise
     */
    @Override
    public boolean isActive() {
        return activeContextIfOwned() != null;
    }

    /**
     * Returns the execution context if it is currently owned by this message stream, otherwise null.
     *
     * <p>
     * The context is read into a local so that callers observe a single, consistent snapshot. The context is nulled by
     * {@link #clearExecutionContext()} on the transport thread when the client stream closes, which can happen while a
     * script evaluation is still resolving resources on another thread; checking the field and then reading through it
     * separately would leave a window for a NullPointerException.
     *
     * @return the execution context owned by this message stream, or null if this stream is not the active one
     */
    private RemoteFileSourceExecutionContext activeContextIfOwned() {
        final RemoteFileSourceExecutionContext context = executionContext;
        return context != null && context.getActiveMessageStream() == this ? context : null;
    }

    /**
     * Checks if this provider has any resource paths configured.
     *
     * @return true if this provider is active and has non-empty resource paths, false otherwise
     */
    @Override
    public boolean hasConfiguredResources() {
        final RemoteFileSourceExecutionContext context = activeContextIfOwned();
        return context != null && !context.getResourcePaths().isEmpty();
    }

    /**
     * Checks if this provider's execution context is dirty.
     *
     * @return true if this provider is active and the execution context is dirty, false otherwise
     */
    @Override
    public boolean isDirty() {
        final RemoteFileSourceExecutionContext context = activeContextIfOwned();
        return context != null && context.isDirty();
    }

    /**
     * Sets the execution context with the active message stream and resource paths.
     *
     * <p>
     * This static method establishes which message stream instance should be considered "active" for resource requests,
     * and which resource paths should be resolved from that remote source. Only one execution context can be active at
     * a time across all instances.
     *
     * <p>
     * In multi-client scenarios (Community Core), this ensures that only the message stream for the currently executing
     * script is active, preventing resource requests from being serviced by the wrong client connection.
     *
     * <p>
     * <b>Typical Usage:</b> Called at the beginning of script execution to establish which .groovy files should be
     * sourced from the remote client rather than the local classpath.
     *
     * @param messageStream the message stream to set as active (must not be null)
     * @param resourcePaths list of resource paths (e.g., "package/MyScript.groovy") to resolve from remote source
     * @param isDirty whether remote sources have changed and cache should be cleared
     * @throws IllegalArgumentException if messageStream is null
     */
    public static void setExecutionContext(RemoteFileSourceMessageStream messageStream, List<String> resourcePaths,
            boolean isDirty) {
        if (messageStream == null) {
            throw new IllegalArgumentException("messageStream must not be null");
        }

        executionContext = new RemoteFileSourceExecutionContext(messageStream, resourcePaths, isDirty);
        log.info().append("Set execution context with ")
                .append(executionContext.getResourcePaths().size()).append(" resource paths")
                .append(", isDirty: ").append(isDirty).endl();
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
    private void handleSetExecutionContext(String requestId, SetExecutionContextRequest setExecutionContext) {
        boolean isDirty = setExecutionContext.getIsDirty();
        List<String> resourcePaths = setExecutionContext.getResourcePathsList();

        setExecutionContext(this, resourcePaths, isDirty);
        log.info().append("Client set execution context for this message stream with ")
                .append(resourcePaths.size()).append(" resource paths")
                .append(", isDirty: ").append(isDirty).endl();

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

        RemoteFileSourceServerMessage serverRequest = RemoteFileSourceServerMessage.newBuilder()
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
     * Handles cleanup when the message stream is closed. Unregisters this provider from the
     * RemoteFileSourceClassLoader, clears the execution context if this was active, and cancels all pending resource
     * requests.
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
        classLoader.registerProvider(this);
        log.info().append("Registered RemoteFileSourceMessageStream provider with RemoteFileSourceClassLoader").endl();
    }

    /**
     * Unregister this message stream instance from the RemoteFileSourceClassLoader.
     */
    private void unregisterFromClassLoader() {
        RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        classLoader.unregisterProvider(this);
        log.info().append("Unregistered RemoteFileSourceMessageStream provider from RemoteFileSourceClassLoader")
                .endl();
    }


    /**
     * Encapsulates the execution context for remote file source operations. This includes the currently active message
     * stream and the resource paths that should be resolved from the remote source. This class is immutable - a new
     * instance is created each time the context changes.
     */
    public static class RemoteFileSourceExecutionContext {
        private final RemoteFileSourceMessageStream activeMessageStream;
        private final List<String> resourcePaths;
        private final boolean isDirty;

        /**
         * Creates a new execution context.
         *
         * @param activeMessageStream the active message stream
         * @param resourcePaths list of resource paths to resolve from remote source
         * @param isDirty whether remote sources have changed and cache should be cleared
         */
        public RemoteFileSourceExecutionContext(RemoteFileSourceMessageStream activeMessageStream,
                List<String> resourcePaths, boolean isDirty) {
            this.activeMessageStream = activeMessageStream;
            this.resourcePaths = resourcePaths;
            this.isDirty = isDirty;
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
         * @return the list of resource paths
         */
        public List<String> getResourcePaths() {
            return resourcePaths;
        }

        /**
         * Gets whether remote sources have changed and cache should be cleared.
         *
         * @return true if dirty, false otherwise
         */
        public boolean isDirty() {
            return isDirty;
        }
    }
}

