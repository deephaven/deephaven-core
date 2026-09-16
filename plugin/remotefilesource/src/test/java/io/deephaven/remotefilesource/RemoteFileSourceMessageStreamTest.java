//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import io.deephaven.engine.util.RemoteFileSourceClassLoader;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceClientMessage;
import io.deephaven.proto.backplane.grpc.SetExecutionContextRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the ownership rules around the process-wide execution context, which decides whose client connection
 * services a Groovy resource request.
 */
class RemoteFileSourceMessageStreamTest {

    @BeforeAll
    static void initializeClassLoader() {
        // The message stream registers itself with the singleton class loader on construction. Nothing else in this
        // test JVM initializes it, since that normally happens when GroovyDeephavenSession is loaded.
        RemoteFileSourceClassLoader.initialize(RemoteFileSourceMessageStreamTest.class.getClassLoader());
    }

    /**
     * A connection that discards everything the server sends it.
     */
    private static final class FakeConnection implements ObjectType.MessageStream {
        @Override
        public void onData(final ByteBuffer payload, final Object... references) {}

        @Override
        public void onClose() {}
    }

    private static RemoteFileSourceMessageStream newStream() throws ObjectCommunicationException {
        return new RemoteFileSourceMessageStream(new FakeConnection());
    }

    /**
     * Sets the execution context the way a client does, by sending a message rather than by calling the static setter.
     */
    private static void sendSetExecutionContext(final RemoteFileSourceMessageStream stream,
            final List<String> resourcePaths) throws ObjectCommunicationException {
        final RemoteFileSourceClientMessage message = RemoteFileSourceClientMessage.newBuilder()
                .setRequestId("test-request")
                .setSetExecutionContext(SetExecutionContextRequest.newBuilder()
                        .setIsDirty(false)
                        .addAllResourcePaths(resourcePaths))
                .build();
        stream.onData(ByteBuffer.wrap(message.toByteArray()));
    }

    @Test
    void setExecutionContextMakesOnlyTheRequestingStreamActive() throws Exception {
        final RemoteFileSourceMessageStream first = newStream();
        final RemoteFileSourceMessageStream second = newStream();
        try {
            sendSetExecutionContext(first, List.of("com/example/First.groovy"));

            assertThat(first.isActive()).isTrue();
            assertThat(second.isActive()).isFalse();
            assertThat(first.canSourceResource("com/example/First.groovy")).isTrue();
            assertThat(second.canSourceResource("com/example/First.groovy")).isFalse();
        } finally {
            first.onClose();
            second.onClose();
        }
    }

    @Test
    void closingTheActiveStreamClearsItsExecutionContext() throws Exception {
        final RemoteFileSourceMessageStream stream = newStream();
        sendSetExecutionContext(stream, List.of("com/example/Only.groovy"));
        assertThat(stream.isActive()).isTrue();

        stream.onClose();

        assertThat(stream.isActive()).isFalse();
        assertThat(RemoteFileSourceClassLoader.getInstance().hasConfiguredRemoteSources()).isFalse();
    }

    /**
     * Closing a superseded stream must leave the new owner's execution context in place.
     *
     * <p>
     * This covers the sequential case only, and passes with or without the compareAndSet in
     * clearExecutionContextIfOwned. Failing without it requires the close and the new setExecutionContext to overlap on
     * two threads, which a test cannot reliably force.
     */
    @Test
    void closingASupersededStreamLeavesTheNewExecutionContextAlone() throws Exception {
        final RemoteFileSourceMessageStream oldStream = newStream();
        final RemoteFileSourceMessageStream newStream = newStream();
        try {
            sendSetExecutionContext(oldStream, List.of("com/example/Old.groovy"));
            sendSetExecutionContext(newStream, List.of("com/example/New.groovy"));

            oldStream.onClose();

            assertThat(newStream.isActive()).isTrue();
            assertThat(newStream.canSourceResource("com/example/New.groovy")).isTrue();
            assertThat(RemoteFileSourceClassLoader.getInstance().hasConfiguredRemoteSources()).isTrue();
        } finally {
            newStream.onClose();
        }
    }

    @Test
    void onlyGroovyResourcesAreSourcedRemotely() throws Exception {
        final RemoteFileSourceMessageStream stream = newStream();
        try {
            sendSetExecutionContext(stream, List.of("com/example/Thing.groovy", "com/example/Thing.class"));

            assertThat(stream.canSourceResource("com/example/Thing.groovy")).isTrue();
            assertThat(stream.canSourceResource("com/example/Thing.class")).isFalse();
            assertThat(stream.canSourceResource("com/example/Unlisted.groovy")).isFalse();
        } finally {
            stream.onClose();
        }
    }
}
