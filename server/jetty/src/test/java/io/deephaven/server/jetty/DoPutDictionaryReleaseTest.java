//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import com.google.protobuf.ByteString;
import io.deephaven.auth.AuthContext;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.extensions.barrage.util.ArrowToTableConverter;
import io.deephaven.server.arrow.ArrowFlightUtil;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Verifies that {@link ArrowFlightUtil.DoPutObserver} returns the dictionary value chunks it decodes to the chunk pools
 * however the DoPut stream ends, and that an abandoned {@link ArrowToTableConverter} can release them too.
 */
public class DoPutDictionaryReleaseTest {

    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();
    private static final Field DICTIONARY_FIELD = new Field("Sym",
            new FieldType(true, ArrowType.Utf8.INSTANCE,
                    new DictionaryEncoding(0L, false, new ArrowType.Int(32, true))),
            Collections.emptyList());

    private SafeCloseable executionContext;
    private SessionState session;
    private BufferAllocator allocator;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        session = new SessionState(new TestControlledScheduler(), new SessionService.ObfuscatingErrorTransformer(),
                TestExecutionContext::createForUnitTests, AUTH_CONTEXT) {
            {
                initializeExpiration(new SessionService.TokenExpiration(UUID.randomUUID(),
                        DateTimeUtils.epochMillis(DateTimeUtils.epochNanosToInstant(Long.MAX_VALUE)), this));
            }
        };
        allocator = new RootAllocator();
    }

    @After
    public void tearDown() {
        allocator.close();
        allocator = null;
        session = null;
        executionContext.close();
        executionContext = null;
    }

    @Test
    public void testOnCompletedReleasesDictionaries() throws IOException {
        runDoPutWithDictionary((doPut, observer) -> {
            try {
                // no flight descriptor was sent, so the export fails; the dictionaries must be released regardless
                doPut.onCompleted();
                fail("Expected onCompleted to reject a DoPut without a flight descriptor");
            } catch (final StatusRuntimeException expected) {
            }
        });
    }

    @Test
    public void testOnErrorReleasesDictionaries() throws IOException {
        runDoPutWithDictionary((doPut, observer) -> doPut.onError(new RuntimeException("client failed")));
    }

    @Test
    public void testOnCancelReleasesDictionaries() throws IOException {
        runDoPutWithDictionary((doPut, observer) -> {
            assertNotNull("DoPutObserver must register a cancel handler", observer.onCancelHandler);
            observer.onCancelHandler.run();
        });
    }

    @Test
    public void testSessionExpirationReleasesDictionaries() throws IOException {
        final InputStream lateDictionaryMessage = flightData(dictionaryBatchMessage("date"));
        runDoPutWithDictionary((doPut, observer) -> {
            // expiration closes the call, after which gRPC calls none of the observer's terminal methods
            session.onExpired();
            // a message already on its way must not decode dictionaries that nothing would release
            doPut.onNext(lateDictionaryMessage);
        });
    }

    @Test
    public void testAbandonedConversionReleasesDictionaries() throws IOException {
        final byte[] schemaMessage = schemaMessage();
        final byte[] dictionaryMessage = dictionaryBatchMessage("apple", "banana", "cherry");

        ChunkPoolReleaseTracking.enableStrict();
        try {
            final ArrowToTableConverter converter = new ArrowToTableConverter();
            converter.setSchema(ByteBuffer.wrap(schemaMessage));
            converter.addRecordBatch(ByteBuffer.wrap(dictionaryMessage));
            // the caller gives up without calling onCompleted()
            converter.releaseDictionaries();
        } finally {
            ChunkPoolReleaseTracking.checkAndDisable();
        }
    }

    /**
     * Sends a schema with a dictionary-encoded column and a DictionaryBatch for it, ends the stream with
     * {@code endStream}, then checks that every pooled chunk taken meanwhile was given back.
     */
    private void runDoPutWithDictionary(
            final BiConsumer<ArrowFlightUtil.DoPutObserver, CapturingServerCallObserver> endStream)
            throws IOException {
        final InputStream schemaMessage = flightData(schemaMessage());
        final InputStream dictionaryMessage = flightData(dictionaryBatchMessage("apple", "banana", "cherry"));

        ChunkPoolReleaseTracking.enableStrict();
        try {
            final CapturingServerCallObserver observer = new CapturingServerCallObserver();
            final ArrowFlightUtil.DoPutObserver doPut = new ArrowFlightUtil.DoPutObserver(
                    session, null, new SessionService.ObfuscatingErrorTransformer(), observer);
            doPut.onNext(schemaMessage);
            doPut.onNext(dictionaryMessage);
            endStream.accept(doPut, observer);
        } finally {
            ChunkPoolReleaseTracking.checkAndDisable();
        }
    }

    private static byte[] schemaMessage() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)),
                new Schema(Collections.singletonList(DICTIONARY_FIELD)));
        return out.toByteArray();
    }

    private byte[] dictionaryBatchMessage(final String... values) throws IOException {
        try (final VarCharVector valuesVector = new VarCharVector("values", allocator)) {
            valuesVector.allocateNew();
            for (int ii = 0; ii < values.length; ++ii) {
                valuesVector.setSafe(ii, values[ii].getBytes(StandardCharsets.UTF_8));
            }
            valuesVector.setValueCount(values.length);
            try (final VectorSchemaRoot root = VectorSchemaRoot.of(valuesVector);
                    final ArrowDictionaryBatch dictionaryBatch =
                            new ArrowDictionaryBatch(0L, new VectorUnloader(root).getRecordBatch(), false)) {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), dictionaryBatch);
                return out.toByteArray();
            }
        }
    }

    /** Wraps an encapsulated Arrow IPC message in the FlightData a DoPut client would send. */
    private static InputStream flightData(final byte[] ipcMessage) {
        // An encapsulated message is a continuation marker, the metadata length, the metadata, then the body.
        final ByteBuffer bb = ByteBuffer.wrap(ipcMessage).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals("continuation marker", -1, bb.getInt(0));
        final int metadataLength = bb.getInt(4);
        final int bodyOffset = 8 + metadataLength;
        return Flight.FlightData.newBuilder()
                .setDataHeader(ByteString.copyFrom(ipcMessage, 8, metadataLength))
                .setDataBody(ByteString.copyFrom(ipcMessage, bodyOffset, ipcMessage.length - bodyOffset))
                .build()
                .toByteString()
                .newInput();
    }

    private static final class CapturingServerCallObserver extends ServerCallStreamObserver<Flight.PutResult> {
        Runnable onCancelHandler;

        @Override
        public void onNext(final Flight.PutResult value) {}

        @Override
        public void onError(final Throwable t) {}

        @Override
        public void onCompleted() {}

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(final Runnable onCancelHandler) {
            this.onCancelHandler = onCancelHandler;
        }

        @Override
        public void setCompression(final String compression) {}

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(final Runnable onReadyHandler) {}

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void request(final int count) {}

        @Override
        public void setMessageCompression(final boolean enable) {}
    }
}
