/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.grpc_api.barrage;

import com.google.common.io.LittleEndianDataOutputStream;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageFieldNode;
import io.deephaven.barrage.flatbuf.BarrageRecordBatch;
import io.deephaven.barrage.flatbuf.Buffer;
import io.deephaven.barrage.flatbuf.FieldNode;
import io.deephaven.barrage.flatbuf.Message;
import io.deephaven.barrage.flatbuf.MessageHeader;
import io.deephaven.barrage.flatbuf.MetadataVersion;
import io.deephaven.barrage.flatbuf.RecordBatch;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.ExternalizableIndexUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.util.SafeCloseable;
import io.deephaven.grpc_api.barrage.util.BarrageSchemaUtil;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil.ExposedByteArrayOutputStream;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.BarrageData;
import io.grpc.Drainable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Map;
import java.util.function.Consumer;

import static io.deephaven.grpc_api_client.barrage.chunk.BaseChunkInputStreamGenerator.PADDING_BUFFER;

public class BarrageStreamGenerator implements BarrageMessageProducer.StreamGenerator<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> {
    private static final Logger log = LoggerFactory.getLogger(BarrageStreamGenerator.class);

    public interface View {
        InputStream getInputStream() throws IOException;
    }

    @Singleton
    public static class Factory implements BarrageMessageProducer.StreamGenerator.Factory<ChunkInputStreamGenerator.Options, View> {
        @Inject
        public Factory() {}

        @Override
        public BarrageMessageProducer.StreamGenerator<ChunkInputStreamGenerator.Options, View> newGenerator(final BarrageMessage message) {
            return new BarrageStreamGenerator(message);
        }

        @Override
        public View getSchemaView(final ChunkInputStreamGenerator.Options options, final TableDefinition table, final Map<String, Object> attributes) {
            final FlatBufferBuilder builder = new FlatBufferBuilder();
            final int schemaOffset = BarrageSchemaUtil.makeSchemaPayload(builder, table, attributes);
            builder.finish(wrapInMessage(builder, schemaOffset, BarrageStreamGenerator.SCHEMA_TYPE_ID));
            return new SchemaView(builder.dataBuffer());
        }
    }

    public static final byte SCHEMA_TYPE_ID = 1;
    public static final byte BARRAGE_RECORD_BATCH_TYPE_ID = 6;
    public static final short BARRAGE_RECORD_BATCH_VERSION = 0;

    public static class ModColumnData {
        public final IndexGenerator rowsModified;
        public final IndexGenerator rowsIncluded;
        public final ChunkInputStreamGenerator data;

        ModColumnData(final BarrageMessage.ModColumnData col) throws IOException {
            rowsModified = new IndexGenerator(col.rowsModified);
            rowsIncluded = new IndexGenerator(col.rowsIncluded);
            data = ChunkInputStreamGenerator.makeInputStreamGenerator(col.data.getChunkType(), col.type, col.data);
        }
    }

    public final BarrageMessage message;

    public final long firstSeq;
    public final long lastSeq;
    public final long step;

    public final boolean isSnapshot;

    public final IndexGenerator rowsAdded;
    public final IndexGenerator rowsIncluded;
    public final IndexGenerator rowsRemoved;
    public final IndexShiftDataGenerator shifted;

    public final ChunkInputStreamGenerator[] addColumnData;
    public final ModColumnData[] modColumnData;

    /**
     * Create a barrage stream generator that can slice and dice the barrage message for delivery to clients.
     *
     * @param message the generator takes ownership of the message and its internal objects
     */
    public BarrageStreamGenerator(final BarrageMessage message) {
        this.message = message;
        try {
            firstSeq = message.firstSeq;
            lastSeq = message.lastSeq;
            step = message.step;
            isSnapshot = message.isSnapshot;

            rowsAdded = new IndexGenerator(message.rowsAdded);
            rowsIncluded = new IndexGenerator(message.rowsIncluded);
            rowsRemoved = new IndexGenerator(message.rowsRemoved);
            shifted = new IndexShiftDataGenerator(message.shifted);

            addColumnData = new ChunkInputStreamGenerator[message.addColumnData.length];
            for (int i = 0; i < message.addColumnData.length; ++i) {
                final BarrageMessage.AddColumnData acd = message.addColumnData[i];
                addColumnData[i] = ChunkInputStreamGenerator.makeInputStreamGenerator(acd.data.getChunkType(), acd.type, acd.data);
            }
            modColumnData = new ModColumnData[message.modColumnData.length];
            for (int i = 0; i < modColumnData.length; ++i) {
                modColumnData[i] = new ModColumnData(message.modColumnData[i]);
            }

            if (message.snapshotIndex != null) {
                message.snapshotIndex.close();
            }
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("unexpected IOException while creating barrage message stream", e);
        }
    }

    @Override
    public BarrageMessage getMessage() {
        return message;
    }

    @Override
    public void close() {
        rowsAdded.close();
        rowsIncluded.close();
        rowsRemoved.close();

        if (addColumnData != null) {
            for (final ChunkInputStreamGenerator in : addColumnData) {
                in.close();
            }
        }
        if (modColumnData != null) {
            for (final ModColumnData mcd : modColumnData) {
                mcd.rowsModified.close();
                mcd.rowsIncluded.close();
                mcd.data.close();
            }
        }
    }

    /**
     * @param options serialization options for this specific view
     * @param isInitialSnapshot is this the first snapshot for the listener
     * @param viewport is the position-space viewport
     * @param keyspaceViewport is the key-space viewport
     * @param subscribedColumns are the columns subscribed for this view
     * @return a view wrapping the input parameters in the context of this generator
     */
    @Override
    public SubView getSubView(final ChunkInputStreamGenerator.Options options,
                           final boolean isInitialSnapshot,
                           @Nullable final Index viewport,
                           @Nullable final Index keyspaceViewport,
                           @Nullable final BitSet subscribedColumns) {
        return new SubView(this, options, isInitialSnapshot, viewport, keyspaceViewport, subscribedColumns);
    }

    public static class SubView implements View {
        public final BarrageStreamGenerator generator;
        public final ChunkInputStreamGenerator.Options options;
        public final boolean isInitialSnapshot;
        public final Index viewport;
        public final Index keyspaceViewport;
        public final BitSet subscribedColumns;

        public SubView(final BarrageStreamGenerator generator,
                       final ChunkInputStreamGenerator.Options options,
                       final boolean isInitialSnapshot,
                       @Nullable final Index viewport,
                       @Nullable final Index keyspaceViewport,
                       @Nullable final BitSet subscribedColumns) {
            this.generator = generator;
            this.options = options;
            this.isInitialSnapshot = isInitialSnapshot;
            this.viewport = viewport;
            this.keyspaceViewport = keyspaceViewport;
            this.subscribedColumns = subscribedColumns;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return generator.getInputStream(this);
        }

        public boolean isViewport() {
            return this.viewport != null;
        }
    }

    public static class SchemaView implements View {
        final byte[] msgBytes;

        public SchemaView(final ByteBuffer buffer) {
            this.msgBytes = BarrageData.newBuilder()
                    .setDataHeader(ByteStringAccess.wrap(buffer))
                    .build()
                    .toByteArray();
        }

        @Override
        public InputStream getInputStream() {
            return new DrainableByteArrayInputStream(msgBytes, 0, msgBytes.length);
        }
    }

    /**
     * Returns an InputStream of the message filtered to the viewport.
     * @return an InputStream ready to be drained by GRPC
     */
    private InputStream getInputStream(final SubView view) throws IOException {
        final ArrayDeque<InputStream> streams = new ArrayDeque<>();
        final MutableInt size = new MutableInt();

        final Consumer<InputStream> addStream = (final InputStream is) -> {
            try {
                final int sz = is.available();
                if (sz == 0) {
                    is.close();
                    return;
                }

                streams.add(is);
                size.add(sz);
            } catch (final IOException e) {
                throw new UncheckedDeephavenException("Unexpected IOException", e);
            }

            // These buffers must be aligned to an 8-byte boundary in order for efficient alignment in languages like C++.
            if (size.intValue() % 8 != 0) {
                final int paddingBytes = (8 - (size.intValue() % 8));
                size.add(paddingBytes);
                streams.add(new DrainableByteArrayInputStream(PADDING_BUFFER, 0, paddingBytes));
            }
        };

        final FlatBufferBuilder builder = new FlatBufferBuilder();

        int effectiveViewportOffset = 0;
        if (isSnapshot && view.isViewport()) {
            try (final IndexGenerator viewportGen = new IndexGenerator(view.viewport)) {
                effectiveViewportOffset = viewportGen.addToFlatBuffer(builder);
            }
        }

        int effectiveColumnSetOffset = 0;
        if (isSnapshot && view.subscribedColumns != null) {
            effectiveColumnSetOffset = new BitSetGenerator(view.subscribedColumns).addToFlatBuffer(builder);
        }

        final int rowsAddedOffset;
        if (isSnapshot && !view.isInitialSnapshot) {
            // client's don't need/want to receive the full index on every snapshot
            rowsAddedOffset = EmptyIndexGenerator.INSTANCE.addToFlatBuffer(builder);
        } else {
            rowsAddedOffset = rowsAdded.addToFlatBuffer(builder);
        }

        final int rowsRemovedOffset = rowsRemoved.addToFlatBuffer(builder);
        final int shiftDataOffset = shifted.addToFlatBuffer(builder);

        // Added Chunk Data:
        final Index myAddedOffsets;
        int addedRowsIncludedOffset = 0;
        if (view.isViewport()) {
            // only include added rows that are within the viewport
            myAddedOffsets = rowsIncluded.original.invert(view.keyspaceViewport.intersect(rowsIncluded.original));
            addedRowsIncludedOffset = rowsIncluded.addToFlatBuffer(view.keyspaceViewport, builder);
        } else if (!rowsAdded.original.equals(rowsIncluded.original)) {
            // there are scoped rows included in the chunks that need to be removed
            myAddedOffsets = rowsIncluded.original.invert(rowsAdded.original);
        } else {
            // use chunk data as-is
            myAddedOffsets = null;
        }

        final int nodesOffset;
        final int buffersOffset;
        final int numOffsets = addColumnData.length + modColumnData.length;
        try (final WritableIntChunk<Attributes.Values> nodeOffsets = WritableIntChunk.makeWritableChunk(numOffsets);
             final WritableObjectChunk<ChunkInputStreamGenerator.BufferInfo, Attributes.Values> bufferInfos = WritableObjectChunk.makeWritableChunk(numOffsets * 3)) {
            nodeOffsets.setSize(0);
            bufferInfos.setSize(0);

            final MutableInt bufferOffset = new MutableInt();
            final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener =
                    (numElements, nullCount) -> nodeOffsets.add(BarrageFieldNode.createBarrageFieldNode(builder, numElements, nullCount, 0, 0));
            final ChunkInputStreamGenerator.BufferListener bufferListener = (offset, length) -> {
                final long myOffset = offset + bufferOffset.getAndAdd(length);
                bufferInfos.add(new ChunkInputStreamGenerator.BufferInfo(myOffset, length));
            };

            // add the add-column streams
            for (final ChunkInputStreamGenerator col : addColumnData) {
                final ChunkInputStreamGenerator.DrainableColumn drainableColumn = col.getInputStream(view.options, myAddedOffsets);
                addStream.accept(drainableColumn);
                drainableColumn.visitFieldNodes(fieldNodeListener);
                drainableColumn.visitBuffers(bufferListener);
            }

            // now add mod-column streams, and write the mod column indexes
            for (final ModColumnData mcd : modColumnData) {
                final int modRowOffset = mcd.rowsModified.addToFlatBuffer(builder);

                Index myModOffsets = null;
                final int myModRowOffset;
                if (view.isViewport()) {
                    // only include added rows that are within the viewport
                    myModOffsets = mcd.rowsIncluded.original.invert(view.keyspaceViewport.intersect(mcd.rowsIncluded.original));
                    myModRowOffset = mcd.rowsIncluded.addToFlatBuffer(view.keyspaceViewport, builder);
                } else {
                    myModRowOffset = 0;
                }

                final ChunkInputStreamGenerator.FieldNodeListener modFieldNodeListener =
                        (numElements, nullCount) -> nodeOffsets.add(BarrageFieldNode.createBarrageFieldNode(builder, numElements, nullCount, modRowOffset, myModRowOffset));

                final ChunkInputStreamGenerator.DrainableColumn drainableColumn =
                        mcd.data.getInputStream(view.options, myModOffsets);

                addStream.accept(drainableColumn);
                drainableColumn.visitFieldNodes(modFieldNodeListener);
                drainableColumn.visitBuffers(bufferListener);
            }

            BarrageRecordBatch.startNodesVector(builder, nodeOffsets.size());
            for (int i = nodeOffsets.size() - 1; i >= 0; --i) {
                builder.addOffset(nodeOffsets.get(i));
            }
            nodesOffset = builder.endVector();

            BarrageRecordBatch.startBuffersVector(builder, bufferInfos.size());
            for (int i = bufferInfos.size() - 1; i >= 0; --i) {
                Buffer.createBuffer(builder, bufferInfos.get(i).offset, bufferInfos.get(i).length);
            }
            buffersOffset = builder.endVector();
        }

        BarrageRecordBatch.startBarrageRecordBatch(builder);
        BarrageRecordBatch.addIsSnapshot(builder, isSnapshot);
        BarrageRecordBatch.addFirstSeq(builder, firstSeq);
        BarrageRecordBatch.addLastSeq(builder, lastSeq);
        BarrageRecordBatch.addEffectiveViewport(builder, effectiveViewportOffset);
        BarrageRecordBatch.addEffectiveColumnSet(builder, effectiveColumnSetOffset);
        BarrageRecordBatch.addAddedRows(builder, rowsAddedOffset);
        BarrageRecordBatch.addRemovedRows(builder, rowsRemovedOffset);
        BarrageRecordBatch.addShiftData(builder, shiftDataOffset);
        BarrageRecordBatch.addAddedRowsIncluded(builder, addedRowsIncludedOffset);
        BarrageRecordBatch.addNodes(builder, nodesOffset);
        BarrageRecordBatch.addBuffers(builder, buffersOffset);
        final int headerOffset = BarrageRecordBatch.endBarrageRecordBatch(builder);

        builder.finish(wrapInMessage(builder, headerOffset, BARRAGE_RECORD_BATCH_TYPE_ID));

        // now create the proto header
        try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
            final CodedOutputStream cos = CodedOutputStream.newInstance(baos);

            cos.writeByteBuffer(BarrageData.DATA_HEADER_FIELD_NUMBER, builder.dataBuffer().slice());

            cos.writeTag(BarrageData.DATA_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            cos.writeUInt32NoTag(size.intValue());
            cos.flush();

            streams.addFirst(new DrainableByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));

            return new ConsecutiveDrainableStreams(streams.toArray(new InputStream[0]));
        } catch (final IOException ex) {
            throw new UncheckedDeephavenException("Unexpected IOException", ex);
        }
    }

    public static int wrapInMessage(final FlatBufferBuilder builder, final int headerOffset, final byte headerType) {
        Message.startMessage(builder);
        Message.addHeaderType(builder, headerType);
        Message.addHeader(builder, headerOffset);
        Message.addVersion(builder, MetadataVersion.V5);
        Message.addBodyLength(builder, 0);
        return Message.endMessage(builder);
    }

    private static int createByteVector(final FlatBufferBuilder builder, final byte[] data, final int offset, final int length) {
        builder.startVector(1, length, 1);

        if (length > 0) {
            builder.prep(1, length - 1);

            for (int i = length - 1; i >= 0; --i) {
                builder.putByte(data[offset + i]);
            }
        }

        return builder.endVector();
    }

    /**
     * Returns an InputStream of the message in a DoGet arrow compatible format.
     * @return an InputStream ready to be drained by GRPC
     */
    public InputStream getDoGetInputStream(final SubView view) throws IOException {
        Assert.assertion(rowsRemoved.original.isEmpty(), "rowsRemoved.original.isEmpty()", "update is not a snapshot");
        Assert.assertion(shifted.original.empty(), "shifted.original.empty()", "update is not a snapshot");
        Assert.eqZero(modColumnData.length, "modColumnData.length");

        final ArrayDeque<InputStream> streams = new ArrayDeque<>();
        final MutableInt size = new MutableInt();

        final Consumer<InputStream> addStream = (final InputStream is) -> {
            streams.add(is);
            try {
                size.add(is.available());
            } catch (final IOException e) {
                throw new UncheckedDeephavenException("Unexpected IOException", e);
            }

            // These buffers must be aligned to an 8-byte boundary in order for efficient alignment in languages like C++.
            if (size.intValue() % 8 != 0) {
                final int paddingBytes = (8 - (size.intValue() % 8));
                size.add(paddingBytes);
                streams.add(new DrainableByteArrayInputStream(PADDING_BUFFER, 0, paddingBytes));
            }
        };

        final FlatBufferBuilder builder = new FlatBufferBuilder();

        // Added Chunk Data:
        final Index myAddedOffsets;
        if (view.isViewport()) {
            // only include added rows that are within the viewport
            myAddedOffsets = rowsIncluded.original.invert(view.keyspaceViewport.intersect(rowsIncluded.original));
        } else if (!rowsAdded.original.equals(rowsIncluded.original)) {
            // there are scoped rows included in the chunks that need to be removed
            myAddedOffsets = rowsIncluded.original.invert(rowsAdded.original);
        } else {
            // use chunk data as-is
            myAddedOffsets = null;
        }

        final int nodesOffset;
        final int buffersOffset;
        final int numOffsets = addColumnData.length;
        try (final WritableObjectChunk<ChunkInputStreamGenerator.FieldNodeInfo, Attributes.Values> nodeInfos = WritableObjectChunk.makeWritableChunk(numOffsets);
             final WritableObjectChunk<ChunkInputStreamGenerator.BufferInfo, Attributes.Values> bufferInfos = WritableObjectChunk.makeWritableChunk(numOffsets * 3)) {
            nodeInfos.setSize(0);
            bufferInfos.setSize(0);

            final MutableInt bufferOffset = new MutableInt();
            final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener =
                    (numElements, nullCount) -> nodeInfos.add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount));
            final ChunkInputStreamGenerator.BufferListener bufferListener = (offset, length) -> {
                final long myOffset = offset + bufferOffset.getAndAdd(length);
                bufferInfos.add(new ChunkInputStreamGenerator.BufferInfo(myOffset, length));
            };

            for (final ChunkInputStreamGenerator column : addColumnData) {
                final ChunkInputStreamGenerator.DrainableColumn drainableColumn = column.getInputStream(view.options, myAddedOffsets);
                addStream.accept(drainableColumn);
                drainableColumn.visitFieldNodes(fieldNodeListener);
                drainableColumn.visitBuffers(bufferListener);
            }

            RecordBatch.startNodesVector(builder, nodeInfos.size());
            for (int i = nodeInfos.size() - 1; i >= 0; --i) {
                FieldNode.createFieldNode(builder, nodeInfos.get(i).numElements, nodeInfos.get(i).nullCount);
            }
            nodesOffset = builder.endVector();

            RecordBatch.startBuffersVector(builder, bufferInfos.size());
            for (int i = bufferInfos.size() - 1; i >= 0; --i) {
                Buffer.createBuffer(builder, bufferInfos.get(i).offset, bufferInfos.get(i).length);
            }
            buffersOffset = builder.endVector();
        }

        RecordBatch.startRecordBatch(builder);
        RecordBatch.addNodes(builder, nodesOffset);
        RecordBatch.addBuffers(builder, buffersOffset);
        RecordBatch.addLength(builder, rowsAdded.original.size());
        final int headerOffset = RecordBatch.endRecordBatch(builder);

        builder.finish(wrapInMessage(builder, headerOffset, MessageHeader.RecordBatch));

        // now create the proto header
        try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
            final CodedOutputStream cos = CodedOutputStream.newInstance(baos);

            cos.writeByteBuffer(BarrageData.DATA_HEADER_FIELD_NUMBER, builder.dataBuffer().slice());

            cos.writeTag(BarrageData.DATA_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            cos.writeUInt32NoTag(size.intValue());
            cos.flush();

            streams.addFirst(new DrainableByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));

            return new ConsecutiveDrainableStreams(streams.toArray(new InputStream[0]));
        } catch (final IOException ex) {
            throw new UncheckedDeephavenException("Unexpected IOException", ex);
        }
    }

    public static abstract class ByteArrayGenerator {
        protected int len;
        protected byte[] raw;

        protected int addToFlatBuffer(final FlatBufferBuilder builder) {
            return createByteVector(builder, raw, 0, len);
        }
    }

    public static class IndexGenerator extends ByteArrayGenerator implements SafeCloseable {
        public final Index original;

        public IndexGenerator(final Index index) throws IOException {
            this.original = index.clone();
            //noinspection UnstableApiUsage
            try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                 final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos)) {
                ExternalizableIndexUtils.writeExternalCompressedDeltas(oos, index);
                oos.flush();
                raw = baos.peekBuffer();
                len = baos.size();
            }
        }

        @Override
        public void close() {
            original.close();
        }

        public DrainableByteArrayInputStream getInputStream() {
            return new DrainableByteArrayInputStream(raw, 0, len);
        }

        /**
         * Appends the intersection of the viewport and the originally provided index.
         * @param viewport the key-space version of the viewport
         * @param builder the flatbuffer builder
         * @return offset of the item in the flatbuffer
         */
        protected int addToFlatBuffer(final Index viewport, final FlatBufferBuilder builder) throws IOException {
            if (original.subsetOf(viewport)) {
                return addToFlatBuffer(builder);
            }

            final int nlen;
            final byte[] nraw;
            //noinspection UnstableApiUsage
            try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                 final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos);
                 final Index viewOfOriginal = original.intersect(viewport)) {
                ExternalizableIndexUtils.writeExternalCompressedDeltas(oos, viewOfOriginal);
                oos.flush();
                nraw = baos.peekBuffer();
                nlen = baos.size();
            }

            return createByteVector(builder, nraw, 0, nlen);
        }
    }

    public static class BitSetGenerator extends ByteArrayGenerator {
        public final BitSet original;

        public BitSetGenerator(final BitSet bitset) throws IOException {
            this.original = bitset == null ? new BitSet() : bitset;
            this.raw = original.toByteArray();
            final int nBits = original.previousSetBit(Integer.MAX_VALUE - 1) + 1;
            this.len = (int) ((long)nBits + 7) / 8;
        }

        public int addToFlatBuffer(final BitSet mine, final FlatBufferBuilder builder) throws IOException {
            if (mine.equals(original)) {
                return addToFlatBuffer(builder);
            }

            final byte[] nraw = mine.toByteArray();
            final int nBits = mine.previousSetBit(Integer.MAX_VALUE - 1) + 1;
            final int nlen = (int) ((long)nBits + 7) / 8;
            return createByteVector(builder, nraw, 0, nlen);
        }
    }

    public static class IndexShiftDataGenerator extends ByteArrayGenerator {
        public final IndexShiftData original;

        public IndexShiftDataGenerator(final IndexShiftData shifted) throws IOException {
            this.original = shifted;

            final Index.SequentialBuilder sRangeBuilder = Index.CURRENT_FACTORY.getSequentialBuilder();
            final Index.SequentialBuilder eRangeBuilder = Index.CURRENT_FACTORY.getSequentialBuilder();
            final Index.SequentialBuilder destBuilder = Index.CURRENT_FACTORY.getSequentialBuilder();

            if (shifted != null) {
                for (int i = 0; i < shifted.size(); ++i) {
                    long s = shifted.getBeginRange(i);
                    final long dt = shifted.getShiftDelta(i);

                    if (dt < 0 && s < -dt) {
                        s = -dt;
                    }

                    sRangeBuilder.appendKey(s);
                    eRangeBuilder.appendKey(shifted.getEndRange(i));
                    destBuilder.appendKey(s + dt);
                }
            }

            //noinspection UnstableApiUsage
            try (final Index sRange = sRangeBuilder.getIndex();
                 final Index eRange = eRangeBuilder.getIndex();
                 final Index dest = destBuilder.getIndex();
                 final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                 final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos)) {
                ExternalizableIndexUtils.writeExternalCompressedDeltas(oos, sRange);
                ExternalizableIndexUtils.writeExternalCompressedDeltas(oos, eRange);
                ExternalizableIndexUtils.writeExternalCompressedDeltas(oos, dest);
                oos.flush();
                raw = baos.peekBuffer();
                len = baos.size();
            }
        }
    }

    public static class DrainableByteArrayInputStream extends ByteArrayInputStream implements Drainable {
        public DrainableByteArrayInputStream(final byte[] buf, final int offset, final int length) {
            super(buf, offset, length);
        }

        @Override
        public synchronized int read() {
            throw new UnsupportedOperationException("to be used as a Drainable only");
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            final int numToDrain = count - pos;
            outputStream.write(buf, pos, numToDrain);
            count += numToDrain;
            return numToDrain;
        }
    }

    private static class ConsecutiveDrainableStreams extends InputStream implements Drainable {
        final InputStream[] streams;

        ConsecutiveDrainableStreams(final InputStream ...streams) {
            this.streams = streams;
            for (final InputStream stream : streams) {
                if (!(stream instanceof Drainable)) {
                    throw new IllegalArgumentException("expecting sub-class of Drainable; found: " + stream.getClass());
                }
            }
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            int total = 0;
            for (final InputStream stream : streams) {
                final int expected = total + stream.available();
                total += ((Drainable)stream).drainTo(outputStream);
                if (expected != total) {
                    throw new IllegalStateException("drained message drained wrong number of bytes");
                }
                if (total < 0) {
                    throw new IllegalStateException("drained message is too large; exceeds Integer.MAX_VALUE");
                }
            }
            return total;
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException("to be used as a Drainable only");
        }

        @Override
        public int available() throws IOException {
            int total = 0;
            for (final InputStream stream : streams) {
                total += stream.available();
                if (total < 0) {
                    throw new IllegalStateException("drained message is too large; exceeds Integer.MAX_VALUE");
                }
            }
            return total;
        }

        @Override
        public void close() throws IOException {
            for (final InputStream stream : streams) {
                try {
                    stream.close();
                } catch (final IOException e) {
                    throw new UncheckedDeephavenException("unexpected IOException", e);
                }
            }
            super.close();
        }
    }

    private static final class EmptyIndexGenerator extends IndexGenerator {
        public static final EmptyIndexGenerator INSTANCE;
        static {
            try {
                INSTANCE = new EmptyIndexGenerator();
            } catch (final IOException ioe) {
                throw new UncheckedDeephavenException(ioe);
            }
        }

        EmptyIndexGenerator() throws IOException {
            super(Index.CURRENT_FACTORY.getEmptyIndex());
        }

        @Override
        public void close() {
            // no-op; this is very re-usable
        }
    }
}
