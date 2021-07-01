/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.arrow;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import com.google.rpc.Code;
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
import io.deephaven.grpc_api.barrage.BarrageStreamReader;
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
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.commons.lang3.mutable.MutableInt;

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
            responseObserver.onNext(ticketRouter.flightInfoFor(request));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getSchema(final Flight.FlightDescriptor request, final StreamObserver<Flight.SchemaResult> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final ByteString schema = ticketRouter.flightInfoFor(request).getSchema();
            responseObserver.onNext(Flight.SchemaResult.newBuilder()
                    .setSchema(schema)
                    .build());
            responseObserver.onCompleted();
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
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no app_metadata provided");
            }
            final ByteBuffer ticketBuf = app_metadata.rpcTicketAsByteBuffer();
            if (ticketBuf == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no rpc ticket provided");
            }

            ticketRouter.publish(session, ticketBuf).submit(() -> {
                final PutMarshaller put = new PutMarshaller(session, FlightServiceGrpcImpl.this, responseObserver);
                put.parseNext(mi);
                return put;
            });
        });
    }

    public void doPutUpdateCustom(final InputStream request, final StreamObserver<Flight.OOBPutResult> responseObserver) {
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
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no app_metadata provided");
            }
            final ByteBuffer ticketBuf = app_metadata.rpcTicketAsByteBuffer();
            if (ticketBuf == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no rpc ticket provided");
            }

            final SessionState.ExportObject<PutMarshaller> putExport = ticketRouter.resolve(session, ticketBuf);

            session.nonExport()
                    .require(putExport)
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        putExport.get().parseNext(mi);
                        responseObserver.onNext(Flight.OOBPutResult.getDefaultInstance()); // nothing to report
                        responseObserver.onCompleted();
                    });
        });
    }

    private static final int BODY_TAG =
            BarrageStreamReader.makeTag(Flight.FlightData.DATA_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    private static final int DATA_HEADER_TAG =
            BarrageStreamReader.makeTag(Flight.FlightData.DATA_HEADER_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    private static final int APP_METADATA_TAG =
            BarrageStreamReader.makeTag(Flight.FlightData.APP_METADATA_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    private static final int FLIGHT_DESCRIPTOR_TAG =
            BarrageStreamReader.makeTag(Flight.FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    private static final BarrageMessage.ModColumnData[] ZERO_MOD_COLUMNS = new BarrageMessage.ModColumnData[0];

    private static MessageInfo parseProtoMessage(final InputStream stream) throws IOException {
        final MessageInfo mi = new MessageInfo();

        final CodedInputStream decoder = CodedInputStream.newInstance(stream);

        for (int tag = decoder.readTag(); tag != 0; tag = decoder.readTag()) {
            if (tag == DATA_HEADER_TAG) {
                final int size = decoder.readRawVarint32();
                mi.header = Message.getRootAsMessage(ByteBuffer.wrap(decoder.readRawBytes(size)));
                continue;
            } else if (tag == APP_METADATA_TAG) {
                final int size = decoder.readRawVarint32();
                mi.app_metadata = BarragePutMetadata.getRootAsBarragePutMetadata(ByteBuffer.wrap(decoder.readRawBytes(size)));
                continue;
            } else if (tag == FLIGHT_DESCRIPTOR_TAG) {
                final int size = decoder.readRawVarint32();
                final byte[] bytes = decoder.readRawBytes(size);
                mi.descriptor = Flight.FlightDescriptor.parseFrom(bytes);
                continue;
            } else if (tag != BODY_TAG) {
                log.info().append("Skipping tag: ").append(tag).endl();
                decoder.skipField(tag);
                continue;
            }

            if (mi.inputStream != null) {
                // latest input stream wins
                mi.inputStream.close();
                mi.inputStream = null;
            }

            final int size = decoder.readRawVarint32();

            //noinspection UnstableApiUsage
            mi.inputStream = new LittleEndianDataInputStream(new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, size));
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
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "already received final app_metadata; cannot apply update");
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
                        nmi = pendingSeq.top();
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
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "only one descriptor definition allowed");
                }
                resultExportBuilder = service.ticketRouter.publish(session, mi.descriptor);
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
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "only schema/record-batch messages supported");
            }

            final int numColumns = resultTable.getColumnSources().size();
            final BarrageMessage msg = new BarrageMessage();
            final RecordBatch batch = (RecordBatch) mi.header.header(new RecordBatch());

            final MutableInt bufferOffset = new MutableInt();
            final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                    new FlatBufferIteratorAdapter<>(batch.nodesLength(), i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));
            final Iterator<ChunkInputStreamGenerator.BufferInfo> bufferInfoIter =
                    new FlatBufferIteratorAdapter<>(batch.buffersLength(), i -> {
                        int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(i).offset());
                        offset -= bufferOffset.getAndAdd(offset);
                        final int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(i).length());
                        return new ChunkInputStreamGenerator.BufferInfo(offset, length);
                    });

            msg.rowsRemoved = Index.FACTORY.getEmptyIndex();
            msg.shifted = IndexShiftData.EMPTY;

            // include all columns as add-columns
            int numRowsAdded = -1;
            msg.addColumnData = new BarrageMessage.AddColumnData[numColumns];
            for (int ci = 0; ci < numColumns; ++ci) {
                final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
                msg.addColumnData[ci] = acd;

                try {
                    acd.data = ChunkInputStreamGenerator.extractChunkFromInputStream(DEFAULT_DESER_OPTIONS, columnChunkTypes[ci], columnTypes[ci], fieldNodeIter, bufferInfoIter, mi.inputStream);
                } catch (final IOException unexpected) {
                    throw new UncheckedDeephavenException(unexpected);
                }

                if (numRowsAdded == -1) {
                    numRowsAdded = acd.data.size();
                } else if (acd.data.size() != numRowsAdded) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "inconsistent num records per column: " + numRowsAdded + " != " + acd.data.size());
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
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "result flight descriptor never provided");
                }
                if (resultTable == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "result flight schema never provided");
                }
                if (pendingSeq != null && !pendingSeq.isEmpty()) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "pending sequences to apply but received final app_metadata");
                }

                resultTable.sealTable(); // no more changes allowed; this is officially static content
                resultExportBuilder.submit(() -> resultTable);

                observer.onCompleted();
                onRequestDone();
            });
        }

        @Override
        public void close() {
            release();
            GrpcUtil.safelyExecute(() -> observer.onError(GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session expired")));
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
