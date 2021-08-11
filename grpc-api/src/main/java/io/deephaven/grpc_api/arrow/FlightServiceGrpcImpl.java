/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.arrow;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import com.google.rpc.Code;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarragePutMetadata;
import io.deephaven.barrage.flatbuf.Message;
import io.deephaven.barrage.flatbuf.MessageHeader;
import io.deephaven.barrage.flatbuf.RecordBatch;
import io.deephaven.barrage.flatbuf.Schema;
import io.deephaven.base.RAPriQueue;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.util.liveness.SingletonLivenessManager;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
import io.deephaven.grpc_api.barrage.util.BarrageSchemaUtil;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.table.BarrageSourcedTable;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.grpc_api_client.util.FlatBufferIteratorAdapter;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.BarrageData;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

@Singleton
public class FlightServiceGrpcImpl extends FlightServiceGrpc.FlightServiceImplBase {
    // TODO (core#412): use app_metadata to communicate serialization options
    private static final ChunkInputStreamGenerator.Options DEFAULT_DESER_OPTIONS = new ChunkInputStreamGenerator.Options.Builder().build();

    private static final Logger log = LoggerFactory.getLogger(FlightServiceGrpcImpl.class);

    private final SessionService sessionService;
    private final TicketRouter ticketRouter;

    @Inject()
    public FlightServiceGrpcImpl(final SessionService sessionService,
                                 final TicketRouter ticketRouter) {
        this.sessionService = sessionService;
        this.ticketRouter = ticketRouter;
    }

    @Override
    public void listFlights(final Flight.Criteria request, final StreamObserver<Flight.FlightInfo> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            ticketRouter.visitFlightInfo(sessionService.getOptionalSession(), responseObserver::onNext);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getFlightInfo(final Flight.FlightDescriptor request, final StreamObserver<Flight.FlightInfo> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getOptionalSession();

            final SessionState.ExportObject<Flight.FlightInfo> export = ticketRouter.flightInfoFor(session, request);

            if (session != null) {
                session.nonExport()
                        .require(export)
                        .onError(responseObserver::onError)
                        .submit(() -> {
                            responseObserver.onNext(export.get());
                            responseObserver.onCompleted();
                        });
            } else {
                if (export.tryRetainReference()) {
                    try {
                        if (export.getState() == ExportNotification.State.EXPORTED) {
                            responseObserver.onNext(export.get());
                            responseObserver.onCompleted();
                        }
                    } finally {
                        export.dropReference();
                    }
                } else {
                    responseObserver.onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Could not find flight info"));
                }
            }
        });
    }

    @Override
    public void getSchema(final Flight.FlightDescriptor request, final StreamObserver<Flight.SchemaResult> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getOptionalSession();

            final SessionState.ExportObject<Flight.FlightInfo> export = ticketRouter.flightInfoFor(session, request);

            if (session != null) {
                session.nonExport()
                        .require(export)
                        .onError(responseObserver::onError)
                        .submit(() -> {
                            responseObserver.onNext(Flight.SchemaResult.newBuilder()
                                    .setSchema(export.get().getSchema())
                                    .build());
                            responseObserver.onCompleted();
                        });
            } else {
                if (export.tryRetainReference()) {
                    try {
                        if (export.getState() == ExportNotification.State.EXPORTED) {
                            responseObserver.onNext(Flight.SchemaResult.newBuilder()
                                    .setSchema(export.get().getSchema())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    } finally {
                        export.dropReference();
                    }
                } else {
                    responseObserver.onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Could not find flight info"));
                }
            }
        });
    }

    public void doGetCustom(final Flight.Ticket request, final StreamObserver<InputStream> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final SessionState.ExportObject<BaseTable> export = ticketRouter.resolve(session, request);
            session.nonExport()
                    .require(export)
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        final BaseTable table = export.get();

                        // Send Schema wrapped in Message
                        final FlatBufferBuilder builder = new FlatBufferBuilder();
                        final int schemaOffset = BarrageSchemaUtil.makeSchemaPayload(builder, table.getDefinition(), table.getAttributes());
                        builder.finish(BarrageStreamGenerator.wrapInMessage(builder, schemaOffset, BarrageStreamGenerator.SCHEMA_TYPE_ID));
                        final ByteBuffer serializedMessage = builder.dataBuffer();

                        final byte[] msgBytes = BarrageData.newBuilder()
                                .setDataHeader(ByteStringAccess.wrap(serializedMessage))
                                .build()
                                .toByteArray();
                        responseObserver.onNext(new BarrageStreamGenerator.DrainableByteArrayInputStream(msgBytes, 0, msgBytes.length));

                        // get ourselves some data!
                        final BarrageMessage msg = ConstructSnapshot.constructBackplaneSnapshot(this, table);
                        msg.modColumnData = new BarrageMessage.ModColumnData[0]; // actually no mod column data for DoGet

                        try (final BarrageStreamGenerator bsg = new BarrageStreamGenerator(msg)) {
                            responseObserver.onNext(bsg.getDoGetInputStream(bsg.getSubView(DEFAULT_DESER_OPTIONS, false, null, null, null)));
                        } catch (final IOException e) {
                            throw new UncheckedDeephavenException(e); // unexpected
                        }

                        responseObserver.onCompleted();
                    });
        });
    }

    public StreamObserver<InputStream> doPutCustom(final StreamObserver<Flight.PutResult> responseObserver) {
        return GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            return new StreamObserver<InputStream>() {
                private final PutMarshaller marshaller = new PutMarshaller(session, FlightServiceGrpcImpl.this, responseObserver);

                @Override
                public void onNext(final InputStream request) {
                    GrpcUtil.rpcWrapper(log, responseObserver, () -> {
                        try {
                            marshaller.parseNext(parseProtoMessage(request));
                        } catch (final IOException unexpected) {
                            throw GrpcUtil.securelyWrapError(log, unexpected);
                        }
                    });
                }

                @Override
                public void onError(final Throwable t) {
                    // ok; we're done then
                    if (marshaller.resultTable != null) {
                        marshaller.resultTable.dropReference();
                        marshaller.resultTable = null;
                    }
                    marshaller.resultExportBuilder.submit(() -> { throw new UncheckedDeephavenException(t); });
                    marshaller.onRequestDone();
                }

                @Override
                public void onCompleted() {
                    marshaller.sealAndExport();
                }
            };
        });
    }

    // client side is out-of-band
    public void doPutCustom(final InputStream request, final StreamObserver<Flight.PutResult> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final MessageInfo mi;
            try {
                mi = parseProtoMessage(request);
            } catch (final IOException unexpected) {
                throw GrpcUtil.securelyWrapError(log, unexpected);
            }
            final BarragePutMetadata app_metadata = mi.app_metadata;
            if (app_metadata == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No app_metadata provided");
            }
            final ByteBuffer ticketBuf = app_metadata.rpcTicketAsByteBuffer();
            if (ticketBuf == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No rpc ticket provided");
            }

            ticketRouter.publish(session, ticketBuf).submit(() -> {
                final PutMarshaller put = new PutMarshaller(session, FlightServiceGrpcImpl.this, responseObserver);
                put.parseNext(mi);
                return put;
            });
        });
    }

    //TODO restore as part of #412
//    public void doPutUpdateCustom(final InputStream request, final StreamObserver<Flight.OOBPutResult> responseObserver) {
//        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
//            final SessionState session = sessionService.getCurrentSession();
//
//            final MessageInfo mi;
//            try {
//                mi = parseProtoMessage(request);
//            } catch (final IOException unexpected) {
//                throw GrpcUtil.securelyWrapError(log, unexpected);
//            }
//
//            final BarragePutMetadata app_metadata = mi.app_metadata;
//            if (app_metadata == null) {
//                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No app_metadata provided");
//            }
//            final ByteBuffer ticketBuf = app_metadata.rpcTicketAsByteBuffer();
//            if (ticketBuf == null) {
//                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No rpc ticket provided");
//            }
//
//            final SessionState.ExportObject<PutMarshaller> putExport = ticketRouter.resolve(session, ticketBuf);
//
//            session.nonExport()
//                    .require(putExport)
//                    .onError(responseObserver::onError)
//                    .submit(() -> {
//                        putExport.get().parseNext(mi);
//                        responseObserver.onNext(Flight.OOBPutResult.getDefaultInstance()); // nothing to report
//                        responseObserver.onCompleted();
//                    });
//        });
//    }

    private static final int TAG_TYPE_BITS = 3;
    private static final int BODY_TAG = (Flight.FlightData.DATA_BODY_FIELD_NUMBER << TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    private static final int DATA_HEADER_TAG = (Flight.FlightData.DATA_HEADER_FIELD_NUMBER << TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    private static final int APP_METADATA_TAG = (Flight.FlightData.APP_METADATA_FIELD_NUMBER << TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    private static final int FLIGHT_DESCRIPTOR_TAG = (Flight.FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER << TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    private static final BarrageMessage.ModColumnData[] ZERO_MOD_COLUMNS = new BarrageMessage.ModColumnData[0];

    private static MessageInfo parseProtoMessage(final InputStream stream) throws IOException {
        final MessageInfo mi = new MessageInfo();

        final CodedInputStream decoder = CodedInputStream.newInstance(stream);

        // if we find a body tag we stop iterating through the loop as there should be no more tags after the body
        // and we lazily drain the payload from the decoder (so the next bytes are payload and not a tag)
        decodeLoop:
        for (int tag = decoder.readTag(); tag != 0; tag = decoder.readTag()) {
            final int size;
            switch (tag) {
                case DATA_HEADER_TAG:
                    size = decoder.readRawVarint32();
                    mi.header = Message.getRootAsMessage(ByteBuffer.wrap(decoder.readRawBytes(size)));
                    break;
                case APP_METADATA_TAG:
                    size = decoder.readRawVarint32();
                    mi.app_metadata = BarragePutMetadata.getRootAsBarragePutMetadata(ByteBuffer.wrap(decoder.readRawBytes(size)));
                    break;
                case FLIGHT_DESCRIPTOR_TAG:
                    size = decoder.readRawVarint32();
                    final byte[] bytes = decoder.readRawBytes(size);
                    mi.descriptor = Flight.FlightDescriptor.parseFrom(bytes);
                    break;
                case BODY_TAG:
                    // at this point, we're in the body, we will read it and then break, the rest of the payload should be the body
                    size = decoder.readRawVarint32();
                    //noinspection UnstableApiUsage
                    mi.inputStream = new LittleEndianDataInputStream(new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, size));
                    // we do not actually remove the content from our stream; prevent reading the next tag via a labeled break
                    break decodeLoop;

                default:
                    log.info().append("Skipping tag: ").append(tag).endl();
                    decoder.skipField(tag);
            }
        }

        if (mi.header != null && mi.header.headerType() == MessageHeader.RecordBatch && mi.inputStream == null) {
            //noinspection UnstableApiUsage
            mi.inputStream = new LittleEndianDataInputStream(new ByteArrayInputStream(CollectionUtil.ZERO_LENGTH_BYTE_ARRAY));
        }

        return mi;
    }

    private static final class MessageInfo {
        /** used for placement in priority queue */
        int pos;

        /** outer-most Arrow Flight Message that indicates the msg type (i.e. schema, record batch, etc) */
        Message header = null;
        /** the embedded flatbuffer metadata indicating information about this batch */
        BarragePutMetadata app_metadata = null;
        /** the parsed protobuf from the flight descriptor embedded in app_metadata */
        Flight.FlightDescriptor descriptor = null;
        /** the payload beyond the header metadata */
        @SuppressWarnings("UnstableApiUsage")
        LittleEndianDataInputStream inputStream = null;
    }

    /**
     * This is a stateful marshaller; a PUT stream begins with its schema.
     */
    private static class PutMarshaller extends SingletonLivenessManager implements Closeable {

        private final SessionState session;
        private final FlightServiceGrpcImpl service;
        private final StreamObserver<Flight.PutResult> observer;

        // TODO (core#29): lift out-of-band processing into a helper utility w/unit tests
        private RAPriQueue<MessageInfo> pendingSeq;

        private long nextSeq = 0;
        private BarrageSourcedTable resultTable;
        private SessionState.ExportBuilder<Table> resultExportBuilder;

        private ChunkType[] columnChunkTypes;
        private Class<?>[] columnTypes;
        private Class<?>[] componentTypes;

        private PutMarshaller(
                final SessionState session,
                final FlightServiceGrpcImpl service,
                final StreamObserver<Flight.PutResult> observer) {
            this.session = session;
            this.service = service;
            this.observer = observer;
            this.session.addOnCloseCallback(this);
            if (observer instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver<Flight.PutResult>) observer).setOnCancelHandler(this::onRequestDone);
            }
        }

        private void parseNext(final MessageInfo mi) {
            GrpcUtil.rpcWrapper(log, observer, () -> {
                synchronized (this) {
                    if (nextSeq == -1) {
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Already received final app_metadata; cannot apply update");
                    }
                    if (mi.app_metadata != null) {
                        final long sequence = mi.app_metadata.sequence();
                        if (sequence != nextSeq) {
                            if (pendingSeq == null) {
                                pendingSeq = new RAPriQueue<>(1, MessageInfoQueueAdapter.INSTANCE, MessageInfo.class);
                            }
                            pendingSeq.enter(mi);
                            return;
                        }
                    }
                }

                MessageInfo nmi = mi;
                do {
                    process(nmi);
                    synchronized (this) {
                        ++nextSeq;
                        nmi = pendingSeq == null ? null : pendingSeq.top();
                        if (nmi == null || nmi.app_metadata.sequence() != nextSeq) {
                            break;
                        }
                        Assert.eq(pendingSeq.removeTop(), "pendingSeq.remoteTop()", nmi, "nmi");
                    }
                } while (true);
            });
        }

        private void process(final MessageInfo mi) {
            if (mi.descriptor != null) {
                if (resultExportBuilder != null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Only one descriptor definition allowed");
                }
                resultExportBuilder = service.ticketRouter
                        .<Table>publish(session, mi.descriptor)
                        .onError(observer::onError);
                manage(resultExportBuilder.getExport());
            }

            if (mi.header == null) {
                return; // nothing to do!
            }

            if (mi.header.headerType() == MessageHeader.Schema) {
                parseSchema((Schema) mi.header.header(new Schema()));
                return;
            }

            if (mi.header.headerType() != MessageHeader.RecordBatch) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Only schema/record-batch messages supported");
            }

            final int numColumns = resultTable.getColumnSources().size();
            final BarrageMessage msg = new BarrageMessage();
            final RecordBatch batch = (RecordBatch) mi.header.header(new RecordBatch());

            final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                    new FlatBufferIteratorAdapter<>(batch.nodesLength(), i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

            final TLongArrayList bufferInfo = new TLongArrayList(batch.buffersLength());
            for (int bi = 0; bi < batch.buffersLength(); ++bi) {
                int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).offset());
                int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).length());

                if (bi < batch.buffersLength() - 1) {
                    final int nextOffset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi + 1).offset());
                    // our parsers handle overhanging buffers
                    length += Math.max(0, nextOffset - offset - length);
                }
                bufferInfo.add(length);
            }
            final TLongIterator bufferInfoIter = bufferInfo.iterator();

            msg.rowsRemoved = Index.FACTORY.getEmptyIndex();
            msg.shifted = IndexShiftData.EMPTY;

            // include all columns as add-columns
            int numRowsAdded = LongSizedDataStructure.intSize("RecordBatch.length()", batch.length());
            msg.addColumnData = new BarrageMessage.AddColumnData[numColumns];
            for (int ci = 0; ci < numColumns; ++ci) {
                final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
                msg.addColumnData[ci] = acd;

                try {
                    acd.data = ChunkInputStreamGenerator.extractChunkFromInputStream(DEFAULT_DESER_OPTIONS, columnChunkTypes[ci], columnTypes[ci], fieldNodeIter, bufferInfoIter, mi.inputStream);
                } catch (final IOException unexpected) {
                    throw new UncheckedDeephavenException(unexpected);
                }

                if (acd.data.size() != numRowsAdded) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Inconsistent num records per column: " + numRowsAdded + " != " + acd.data.size());
                }
                acd.type = columnTypes[ci];
                acd.componentType = componentTypes[ci];
            }

            msg.rowsAdded = Index.FACTORY.getIndexByRange(resultTable.size(), resultTable.size() + numRowsAdded - 1);
            msg.rowsIncluded = msg.rowsAdded.clone();
            msg.modColumnData = ZERO_MOD_COLUMNS;

            resultTable.handleBarrageMessage(msg);

            // no app_metadata to report; but ack the processing
            observer.onNext(Flight.PutResult.newBuilder().build());
        }

        private void sealAndExport() {
            GrpcUtil.rpcWrapper(log, observer, () -> {
                if (resultExportBuilder == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Result flight descriptor never provided");
                }
                if (resultTable == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Result flight schema never provided");
                }
                if (pendingSeq != null && !pendingSeq.isEmpty()) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Pending sequences to apply but received final app_metadata");
                }

                // no more changes allowed; this is officially static content
                resultTable.sealTable(() -> resultExportBuilder.submit(() -> {
                    // transfer ownership to submit's liveness scope, drop our extra reference
                    resultTable.manageWithCurrentScope();
                    resultTable.dropReference();
                    GrpcUtil.safelyExecute(observer::onCompleted);
                    return resultTable;
                }), () -> GrpcUtil.safelyExecute(() -> {
                    observer.onError(GrpcUtil.statusRuntimeException(Code.INTERNAL, "Do put could not be sealed"));
                }));

                onRequestDone();
            });
        }

        @Override
        public void close() {
            release();
            GrpcUtil.safelyExecute(() -> observer.onError(GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "Session expired")));
        }

        private void onRequestDone() {
            nextSeq = -1;
            if (session.removeOnCloseCallback(this) != null) {
                release();
            }
        }

        private void parseSchema(final Schema header) {
            if (resultTable != null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Schema evolution not supported");
            }
            resultTable = BarrageSourcedTable.make(BarrageSchemaUtil.schemaToTableDefinition(header), false);
            columnChunkTypes = resultTable.getWireChunkTypes();
            columnTypes = resultTable.getWireTypes();
            componentTypes = resultTable.getWireComponentTypes();

            // retain reference until we can pass this result to be owned by the export object
            resultTable.retainReference();
        }
    }

    private static class MessageInfoQueueAdapter implements RAPriQueue.Adapter<MessageInfo> {
        private static final MessageInfoQueueAdapter INSTANCE = new MessageInfoQueueAdapter();

        @Override
        public boolean less(MessageInfo a, MessageInfo b) {
            return a.app_metadata.sequence() < b.app_metadata.sequence();
        }

        @Override
        public void setPos(MessageInfo mi, int pos) {
            mi.pos = pos;
        }

        @Override
        public int getPos(MessageInfo mi) {
            return mi.pos;
        }
    }
}
