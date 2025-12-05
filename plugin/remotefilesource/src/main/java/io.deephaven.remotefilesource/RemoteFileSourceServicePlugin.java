//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.auto.service.AutoService;
import com.google.protobuf.InvalidProtocolBufferException;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceClientRequest;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaRequest;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaResponse;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceServerRequest;
import io.deephaven.proto.backplane.grpc.SetExecutionContextResponse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@AutoService(ObjectType.class)
public class RemoteFileSourceServicePlugin extends ObjectTypeBase {
    private static final Logger log = LoggerFactory.getLogger(RemoteFileSourceServicePlugin.class);

    /**
     * The current execution context containing the active message stream and configuration.
     * Null when no execution context is active.
     */
    private static volatile RemoteFileSourceExecutionContext executionContext;

    private volatile RemoteFileSourceMessageStream messageStream;

    public RemoteFileSourceServicePlugin() {
    }

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
        executionContext = null;
        log.info().append("Cleared execution context").endl();
    }

    /**
     * Gets the current execution context.
     *
     * @return the execution context
     */
    public static RemoteFileSourceExecutionContext getExecutionContext() {
        return executionContext;
    }

    @Override
    public String name() {
        return "RemoteFileSourceService";
    }

    @Override
    public boolean isType(Object object) {
        return object instanceof RemoteFileSourceServicePlugin;
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException {
        connection.onData(ByteBuffer.allocate(0));
        messageStream = new RemoteFileSourceMessageStream(connection);
        return messageStream;
    }

    /**
     * Test method to trigger a resource request from the server to the client. Can be called from the console to test
     * bidirectional communication. Usage from console:
     * <pre>
     * service = remote_file_source_service  # The plugin instance
     * service.testRequestResource("com/example/MyClass.java")
     * </pre>
     * @param resourceName the resource to request from the client
     */
    public void testRequestResource(String resourceName) {
        if (messageStream == null) {
            log.error().append("MessageStream not connected. Please connect a client first.").endl();
            return;
        }
        messageStream.testRequestResource(resourceName);
    }

    /**
     * A message stream for the RemoteFileSourceService.
     */
    public static class RemoteFileSourceMessageStream implements MessageStream {
        private final MessageStream connection;
        private final Map<String, CompletableFuture<byte[]>> pendingRequests = new ConcurrentHashMap<>();

        public RemoteFileSourceMessageStream(final MessageStream connection) {
            this.connection = connection;
        }

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

                        // Complete the future - the caller will log the content if needed
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
         * Request a resource from the client.
         *
         * @param resourceName the name/path of the resource to request
         * @return a future that completes with the resource content, or empty array if not found
         */
        public CompletableFuture<byte[]> requestResource(String resourceName) {
            String requestId = UUID.randomUUID().toString();
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            pendingRequests.put(requestId, future);

            try {
                // Build RemoteFileSourceMetaRequest proto
                RemoteFileSourceMetaRequest metaRequest = RemoteFileSourceMetaRequest.newBuilder()
                        .setResourceName(resourceName)
                        .build();

                // Wrap in RemoteFileSourceServerRequest (server→client)
                RemoteFileSourceServerRequest message = RemoteFileSourceServerRequest.newBuilder()
                        .setRequestId(requestId)
                        .setMetaRequest(metaRequest)
                        .build();

                ByteBuffer buffer = ByteBuffer.wrap(message.toByteArray());

                log.info().append("Sending resource request for: ").append(resourceName)
                        .append(" with requestId: ").append(requestId).endl();

                connection.onData(buffer);
            } catch (ObjectCommunicationException e) {
                future.completeExceptionally(e);
                pendingRequests.remove(requestId);
            }

            return future;
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
