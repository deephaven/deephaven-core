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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a client's declared resources are scoped to the single script evaluation they were declared for.
 */
class RemoteFileSourceMessageStreamTest {

    private static final String RESOURCE = "com/example/Thing.groovy";
    private static final String OTHER_RESOURCE = "com/example/Other.groovy";

    @BeforeAll
    static void initializeClassLoader() {
        // Nothing else in this test JVM initializes the class loader, since that normally happens when
        // GroovyDeephavenSession is loaded.
        RemoteFileSourceClassLoader.initialize(RemoteFileSourceMessageStreamTest.class.getClassLoader());
    }

    @BeforeEach
    void clearAnyLeftoverDeclaration() {
        // Each test starts from an evaluation that claimed nothing
        RemoteFileSourceClassLoader.getInstance().beginEvaluation();
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

    /**
     * A connection whose sends fail once {@code sendsFail} is set, leaving the constructor's initial message to
     * succeed.
     */
    private static final class FailingConnection implements ObjectType.MessageStream {
        private boolean sendsFail;

        @Override
        public void onData(final ByteBuffer payload, final Object... references)
                throws ObjectCommunicationException {
            if (sendsFail) {
                throw new ObjectCommunicationException("send failed");
            }
        }

        @Override
        public void onClose() {}
    }

    private static RemoteFileSourceMessageStream newStream() throws ObjectCommunicationException {
        return new RemoteFileSourceMessageStream(new FakeConnection());
    }

    /**
     * Declares resources the way a client does, by sending a message rather than by calling the class loader directly.
     */
    private static void sendSetExecutionContext(final RemoteFileSourceMessageStream stream,
            final List<String> resourcePaths, final boolean dirty) throws ObjectCommunicationException {
        final RemoteFileSourceClientMessage message = RemoteFileSourceClientMessage.newBuilder()
                .setRequestId("test-request")
                .setSetExecutionContext(SetExecutionContextRequest.newBuilder()
                        .setIsDirty(dirty)
                        .addAllResourcePaths(resourcePaths))
                .build();
        stream.onData(ByteBuffer.wrap(message.toByteArray()));
    }

    @Test
    void anEvaluationClaimsTheDeclarationMadeForIt() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream stream = newStream();
        try {
            sendSetExecutionContext(stream, List.of(RESOURCE), false);
            classLoader.beginEvaluation();

            assertThat(classLoader.hasConfiguredRemoteSources()).isTrue();
            assertThat(classLoader.getResource(RESOURCE).getProtocol()).isEqualTo("remotefile");
        } finally {
            stream.onClose();
        }
    }

    /**
     * A run that was not preceded by a declaration sources nothing, even though another client is connected and has
     * declared for its own run. A web IDE script run takes this path, since that client never connects to this plugin.
     */
    @Test
    void anEvaluationWithNoDeclarationSourcesNothing() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream stream = newStream();
        try {
            sendSetExecutionContext(stream, List.of(RESOURCE), false);

            // The declaring client's own run consumes it
            classLoader.beginEvaluation();
            assertThat(classLoader.hasConfiguredRemoteSources()).isTrue();

            // A second run, by anyone, inherits nothing, and must not reuse what the first run compiled
            assertThat(classLoader.beginEvaluation()).isTrue();
            assertThat(classLoader.hasConfiguredRemoteSources()).isFalse();
        } finally {
            stream.onClose();
        }
    }

    /**
     * A declaration arriving while an evaluation is underway must not retarget that evaluation; it belongs to the run
     * that begins next.
     */
    @Test
    void aDeclarationDuringAnEvaluationDoesNotRetargetIt() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream running = newStream();
        final RemoteFileSourceMessageStream other = newStream();
        try {
            sendSetExecutionContext(running, List.of(RESOURCE), false);
            classLoader.beginEvaluation();

            // Another connection declares mid-run, with different sources
            sendSetExecutionContext(other, List.of(OTHER_RESOURCE), false);

            // The running evaluation still resolves what it claimed, and not the newer declaration
            assertThat(classLoader.getResource(RESOURCE).getProtocol()).isEqualTo("remotefile");
            assertThat(classLoader.getResource(OTHER_RESOURCE)).isNull();

            // The newer declaration takes effect for the next run
            classLoader.beginEvaluation();
            assertThat(classLoader.getResource(RESOURCE)).isNull();
            assertThat(classLoader.getResource(OTHER_RESOURCE).getProtocol()).isEqualTo("remotefile");
        } finally {
            running.onClose();
            other.onClose();
        }
    }

    /**
     * Compiled output may be reused only when the run that produced it resolved against the same sources. The three
     * tests below each move one input: the dirty flag, the serving client, or neither.
     */
    @Test
    void sourcesUnchangedWhenTheSameClientRedeclaresUnchanged() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream stream = newStream();
        try {
            sendSetExecutionContext(stream, List.of(RESOURCE), false);
            classLoader.beginEvaluation();

            sendSetExecutionContext(stream, List.of(RESOURCE), false);
            assertThat(classLoader.beginEvaluation()).isFalse();
        } finally {
            stream.onClose();
        }
    }

    @Test
    void sourcesChangedWhenTheSameClientDeclaresDirty() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream stream = newStream();
        try {
            sendSetExecutionContext(stream, List.of(RESOURCE), false);
            classLoader.beginEvaluation();

            // Same client, same paths; only the dirty flag differs
            sendSetExecutionContext(stream, List.of(RESOURCE), true);
            assertThat(classLoader.beginEvaluation()).isTrue();
        } finally {
            stream.onClose();
        }
    }

    /**
     * A client's dirty flag describes only its own sources, so a clean declaration from a different client must still
     * invalidate what the previous client's sources compiled to.
     */
    @Test
    void sourcesChangedWhenADifferentClientServesTheRun() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream first = newStream();
        final RemoteFileSourceMessageStream second = newStream();
        try {
            sendSetExecutionContext(first, List.of(RESOURCE), false);
            classLoader.beginEvaluation();

            // Same paths and still clean; only the serving client differs
            sendSetExecutionContext(second, List.of(RESOURCE), false);
            assertThat(classLoader.beginEvaluation()).isTrue();
        } finally {
            first.onClose();
            second.onClose();
        }
    }

    /**
     * A declaration with no paths serves nothing, so it is indistinguishable from not having declared and leaves
     * compiled output from an equally local run reusable.
     */
    @Test
    void aDeclarationWithNoPathsIsTreatedAsNoDeclaration() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream stream = newStream();
        try {
            sendSetExecutionContext(stream, List.of(), false);

            assertThat(classLoader.beginEvaluation()).isFalse();
            assertThat(classLoader.hasConfiguredRemoteSources()).isFalse();
        } finally {
            stream.onClose();
        }
    }

    @Test
    void onlyDeclaredGroovyResourcesAreSourcedRemotely() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream stream = newStream();
        try {
            sendSetExecutionContext(stream, List.of(RESOURCE, "com/example/Thing.class"), false);
            classLoader.beginEvaluation();

            assertThat(classLoader.getResource(RESOURCE).getProtocol()).isEqualTo("remotefile");
            // Compiled classes are always local, even when declared
            assertThat(classLoader.getResource("com/example/Thing.class")).isNull();
            assertThat(classLoader.getResource("com/example/Undeclared.groovy")).isNull();
        } finally {
            stream.onClose();
        }
    }

    @Test
    void closingAConnectionDropsItsOwnDeclaration() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream stream = newStream();

        sendSetExecutionContext(stream, List.of(RESOURCE), false);
        stream.onClose();

        classLoader.beginEvaluation();
        assertThat(classLoader.hasConfiguredRemoteSources()).isFalse();
    }

    /**
     * A close arrives on the transport error path, so it can land after another connection has declared. The late close
     * must drop only its own declaration.
     */
    @Test
    void closingASupersededConnectionLeavesTheNewDeclarationAlone() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream closing = newStream();
        final RemoteFileSourceMessageStream surviving = newStream();
        try {
            sendSetExecutionContext(closing, List.of(RESOURCE), false);
            sendSetExecutionContext(surviving, List.of(OTHER_RESOURCE), false);

            closing.onClose();

            classLoader.beginEvaluation();
            assertThat(classLoader.getResource(OTHER_RESOURCE).getProtocol()).isEqualTo("remotefile");
            assertThat(classLoader.getResource(RESOURCE)).isNull();
        } finally {
            surviving.onClose();
        }
    }

    /**
     * A client disconnecting mid-evaluation must not quietly change where that evaluation's declared paths come from.
     * The claimed declaration stays in place so the fetch fails, rather than resolving a same-named classpath resource
     * and compiling sources of mixed origin.
     */
    @Test
    void disconnectingDuringAnEvaluationFailsRatherThanResolvingLocally() throws Exception {
        final RemoteFileSourceClassLoader classLoader = RemoteFileSourceClassLoader.getInstance();
        final RemoteFileSourceMessageStream stream = newStream();

        sendSetExecutionContext(stream, List.of(RESOURCE), false);
        classLoader.beginEvaluation();

        stream.onClose();

        final URL url = classLoader.getResource(RESOURCE);
        assertThat(url).isNotNull();
        assertThat(url.getProtocol()).isEqualTo("remotefile");
        // Fails outright, and without waiting for the fetch timeout, since the connection is known to be gone
        assertThatThrownBy(url::openStream).isInstanceOf(IOException.class);
    }

    /**
     * A failed acknowledgment must reach the object service, which closes the stream in response. Swallowing it would
     * leave the declaration installed and claimable while the client waited out its timeout.
     */
    @Test
    void aFailedAcknowledgmentPropagates() throws Exception {
        final FailingConnection connection = new FailingConnection();
        final RemoteFileSourceMessageStream stream = new RemoteFileSourceMessageStream(connection);
        try {
            connection.sendsFail = true;

            assertThatThrownBy(() -> sendSetExecutionContext(stream, List.of(RESOURCE), false))
                    .isInstanceOf(ObjectCommunicationException.class);
        } finally {
            stream.onClose();
        }
    }
}
