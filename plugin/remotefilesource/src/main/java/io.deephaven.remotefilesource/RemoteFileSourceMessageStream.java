//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.protobuf.InvalidProtocolBufferException;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceClientRequest;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaResponse;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceServerRequest;
import io.deephaven.proto.backplane.grpc.SetExecutionContextResponse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Message stream implementation for RemoteFileSource bidirectional communication.
 * Each instance represents a file source provider for one client connection and implements
 * RemoteFileSourceProvider so it can be registered with the ClassLoader.
 * Only one MessageStream can be "active" at a time (determined by the execution context).
 * The ClassLoader checks isActive() on each registered provider to find the active one.
 */
public class RemoteFileSourceMessageStream implements ObjectType.MessageStream, io.deephaven.engine.util.RemoteFileSourceProvider {
    private static final Logger log = LoggerFactory.getLogger(RemoteFileSourceMessageStream.class);

    /**
     * The current execution context containing the active message stream and configuration.
     * Null when no execution context is active.
     * This is accessed by RemoteFileSourcePlugin.PROVIDER to route resource requests to the
     * currently active message stream.
     */
    private static volatile RemoteFileSourceExecutionContext executionContext;


    private final ObjectType.MessageStream connection;
    private final Map<String, CompletableFuture<byte[]>> pendingRequests = new ConcurrentHashMap<>();

    public RemoteFileSourceMessageStream(final ObjectType.MessageStream connection) {
        this.connection = connection;
        // Register this instance as a provider with the ClassLoader
        registerWithClassLoader();
    }

    // RemoteFileSourceProvider interface implementation - each instance is a provider

    @Override
    public java.util.concurrent.CompletableFuture<Boolean> canSourceResource(String resourceName) {
        // Only active if this instance is the currently active message stream
        if (!isActive()) {
            return java.util.concurrent.CompletableFuture.completedFuture(false);
        }

        // Only handle .groovy source files, not compiled .class files
        if (!resourceName.endsWith(".groovy")) {
            return java.util.concurrent.CompletableFuture.completedFuture(false);
        }

        RemoteFileSourceExecutionContext context = executionContext;
        if (context == null || context.getActiveMessageStream() != this) {
            return java.util.concurrent.CompletableFuture.completedFuture(false);
        }

        java.util.List<String> topLevelPackages = context.getTopLevelPackages();
        if (topLevelPackages.isEmpty()) {
            return java.util.concurrent.CompletableFuture.completedFuture(false);
        }

        String resourcePath = resourceName.replace('\\', '/');

        for (String topLevelPackage : topLevelPackages) {
            String packagePath = topLevelPackage.replace('.', '/');
            if (resourcePath.startsWith(packagePath + "/") || resourcePath.startsWith(packagePath)) {
                log.info().append("✅ Can source: ").append(resourceName).endl();
                return java.util.concurrent.CompletableFuture.completedFuture(true);
            }
        }

        return java.util.concurrent.CompletableFuture.completedFuture(false);
    }

    @Override
    public java.util.concurrent.CompletableFuture<byte[]> requestResource(String resourceName) {
        // Only service requests if this instance is active
        if (!isActive()) {
            log.warn().append("Request for resource ").append(resourceName)
                    .append(" on inactive message stream").endl();
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }

        log.info().append("📥 Requesting resource: ").append(resourceName).endl();

        String requestId = java.util.UUID.randomUUID().toString();
        java.util.concurrent.CompletableFuture<byte[]> future = new java.util.concurrent.CompletableFuture<>();
        pendingRequests.put(requestId, future);

        try {
            // Build RemoteFileSourceMetaRequest proto
            io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaRequest metaRequest =
                    io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaRequest.newBuilder()
                    .setResourceName(resourceName)
                    .build();

            // Wrap in RemoteFileSourceServerRequest (server→client)
            io.deephaven.proto.backplane.grpc.RemoteFileSourceServerRequest message =
                    io.deephaven.proto.backplane.grpc.RemoteFileSourceServerRequest.newBuilder()
                    .setRequestId(requestId)
                    .setMetaRequest(metaRequest)
                    .build();

            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(message.toByteArray());

            log.info().append("Sending resource request for: ").append(resourceName)
                    .append(" with requestId: ").append(requestId).endl();

            connection.onData(buffer);
        } catch (ObjectCommunicationException e) {
            future.completeExceptionally(e);
            pendingRequests.remove(requestId);
        }

        return future;
    }

    @Override
    public boolean isActive() {
        RemoteFileSourceExecutionContext context = executionContext;
        return context != null && context.getActiveMessageStream() == this;
    }

    // Static methods for execution context management

    /**
     * Sets the execution context with the active message stream and top-level packages.
     * This should be called when a script execution begins.
     *
     * @param messageStream the message stream to set as active (must not be null)
     * @param packages list of top-level package names to resolve from remote source
     * @throws IllegalArgumentException if messageStream is null (use clearExecutionContext() instead)
     */
    public static void setExecutionContext(RemoteFileSourceMessageStream messageStream, java.util.List<String> packages) {
        if (messageStream == null) {
            throw new IllegalArgumentException("messageStream must not be null. Use clearExecutionContext() to clear the context.");
        }

        executionContext = new RemoteFileSourceExecutionContext(messageStream, packages);
        log.info().append("Set execution context with ")
                .append(packages != null ? packages.size() : 0).append(" top-level packages").endl();
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

    // Instance methods for MessageStream implementation

    @Override
    public void onData(ByteBuffer payload, Object... references) throws ObjectCommunicationException {
        try {
            // Parse as RemoteFileSourceClientRequest proto (client→server)
            byte[] bytes = new byte[payload.remaining()];
            payload.get(bytes);
            RemoteFileSourceClientRequest message = RemoteFileSourceClientRequest.parseFrom(bytes);

            String requestId = message.getRequestId();

            if (message.hasMetaResponse()) {
                // Client is responding to a resource request
                RemoteFileSourceMetaResponse response = message.getMetaResponse();

                CompletableFuture<byte[]> future = pendingRequests.remove(requestId);
                if (future != null) {
                    byte[] content = response.getContent().toByteArray();

                    log.info().append("Received resource response for requestId: ").append(requestId)
                            .append(", found: ").append(response.getFound())
                            .append(", content length: ").append(content.length).endl();

                    if (!response.getError().isEmpty()) {
                        log.warn().append("Error in response: ").append(response.getError()).endl();
                    }

                    future.complete(content);
                } else {
                    log.warn().append("Received response for unknown requestId: ").append(requestId).endl();
                }
            } else if (message.hasTestCommand()) {
                // Client sent a test command
                String command = message.getTestCommand();
                log.info().append("Received test command from client: ").append(command).endl();

                if (command.startsWith("TEST:")) {
                    String resourceName = command.substring(5).trim();
                    log.info().append("Client initiated test for resource: ").append(resourceName).endl();
                    testRequestResource(resourceName);
                }
            } else if (message.hasSetExecutionContext()) {
                // Client is requesting this message stream to become active
                java.util.List<String> packages = message.getSetExecutionContext().getTopLevelPackagesList();
                setExecutionContext(this, packages);
                log.info().append("Client set execution context for this message stream with ")
                        .append(packages.size()).append(" top-level packages").endl();

                // Send acknowledgment back to client
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
            } else {
                log.warn().append("Received unknown message type from client").endl();
            }
        } catch (InvalidProtocolBufferException e) {
            log.error().append("Failed to parse RemoteFileSourceClientRequest: ").append(e).endl();
            throw new ObjectCommunicationException("Failed to parse message", e);
        }
    }

    @Override
    public void onClose() {
        // Unregister this provider from the ClassLoader
        unregisterFromClassLoader();

        // Clear execution context if this was the active stream
        RemoteFileSourceExecutionContext context = executionContext;
        if (context != null && context.getActiveMessageStream() == this) {
            clearExecutionContext();
        }

        // Cancel all pending requests
        pendingRequests.values().forEach(future -> future.cancel(true));
        pendingRequests.clear();
    }

    /**
     * Register this message stream instance as a provider with the ClassLoader.
     */
    private void registerWithClassLoader() {
        io.deephaven.engine.util.RemoteFileSourceClassLoader classLoader =
                io.deephaven.engine.util.RemoteFileSourceClassLoader.getInstance();

        if (classLoader != null) {
            classLoader.registerProvider(this);
            log.info().append("✅ Registered RemoteFileSourceMessageStream provider with ClassLoader").endl();
        } else {
            log.warn().append("⚠️ RemoteFileSourceClassLoader not available").endl();
        }
    }

    /**
     * Unregister this message stream instance from the ClassLoader.
     */
    private void unregisterFromClassLoader() {
        io.deephaven.engine.util.RemoteFileSourceClassLoader classLoader =
                io.deephaven.engine.util.RemoteFileSourceClassLoader.getInstance();

        if (classLoader != null) {
            classLoader.unregisterProvider(this);
            log.info().append("🔴 Unregistered RemoteFileSourceMessageStream provider from ClassLoader").endl();
        }
    }

    /**
     * Test method to request a resource and log the result. This can be called from the server console to test the
     * bidirectional communication.
     *
     * @param resourceName the resource to request
     */
    public void testRequestResource(String resourceName) {
        log.info().append("Testing resource request for: ").append(resourceName).endl();

        requestResource(resourceName)
                .orTimeout(30, TimeUnit.SECONDS)
                .whenComplete((content, error) -> {
                    if (error != null) {
                        log.error().append("Error requesting resource ").append(resourceName)
                                .append(": ").append(error).endl();
                    } else {
                        log.info().append("Successfully received resource ").append(resourceName)
                                .append(" (").append(content.length).append(" bytes)").endl();
                        if (content.length > 0 && content.length < 1000) {
                            String contentStr = new String(content, StandardCharsets.UTF_8);
                            log.info().append("Resource content:\n").append(contentStr).endl();
                        }
                    }
                });
    }

    /**
     * Encapsulates the execution context for remote file source operations.
     * This includes the currently active message stream and the top-level packages
     * that should be resolved from the remote source.
     * This class is immutable - a new instance is created each time the context changes.
     */
    public static class RemoteFileSourceExecutionContext {
        private final RemoteFileSourceMessageStream activeMessageStream;
        private final java.util.List<String> topLevelPackages;

        /**
         * Creates a new execution context.
         *
         * @param activeMessageStream the active message stream
         * @param topLevelPackages list of top-level package names to resolve from remote source
         */
        public RemoteFileSourceExecutionContext(RemoteFileSourceMessageStream activeMessageStream,
                java.util.List<String> topLevelPackages) {
            this.activeMessageStream = activeMessageStream;
            this.topLevelPackages = topLevelPackages != null ? topLevelPackages : java.util.Collections.emptyList();
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
         * Gets the top-level package names that should be resolved from the remote source.
         *
         * @return a copy of the list of top-level package names
         */
        public java.util.List<String> getTopLevelPackages() {
            return new java.util.ArrayList<>(topLevelPackages);
        }
    }
}

