//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.DefaultChunkReadingFactory;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.io.streams.ByteBufferInputStream;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

import static io.deephaven.extensions.barrage.chunk.ChunkReader.typeInfo;
import static io.deephaven.extensions.barrage.util.BarrageProtoUtil.DEFAULT_SER_OPTIONS;

/**
 * This class allows the incremental making of a BarrageTable from Arrow IPC messages, starting with an Arrow Schema
 * message followed by zero or more RecordBatches
 */
public class ArrowToTableConverter {
    protected long totalRowsRead = 0;
    protected BarrageTable resultTable;
    private Class<?>[] columnTypes;
    private Class<?>[] componentTypes;
    protected BarrageSubscriptionOptions options = DEFAULT_SER_OPTIONS;
    private final List<ChunkReader> readers = new ArrayList<>();

    private volatile boolean completed = false;

    public static BarrageProtoUtil.MessageInfo parseArrowIpcMessage(final ByteBuffer bb) {
        final BarrageProtoUtil.MessageInfo mi = new BarrageProtoUtil.MessageInfo();

        bb.order(ByteOrder.LITTLE_ENDIAN);
        final int continuation = bb.getInt();
        final int metadata_size = bb.getInt();
        mi.header = Message.getRootAsMessage(bb);

        if (mi.header.headerType() == MessageHeader.RecordBatch) {
            bb.position(metadata_size + 8);
            final ByteBuffer bodyBB = bb.slice();
            final ByteBufferInputStream bbis = new ByteBufferInputStream(bodyBB);
            final CodedInputStream decoder = CodedInputStream.newInstance(bbis);
            mi.inputStream = new LittleEndianDataInputStream(
                    new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, bodyBB.remaining()));
        }
        return mi;
    }

    public static Schema parseArrowSchema(final BarrageProtoUtil.MessageInfo mi) {
        if (mi.header.headerType() != MessageHeader.Schema) {
            throw new IllegalArgumentException("The input is not a valid Arrow Schema IPC message");
        }

        // The Schema instance (especially originated from Python) can't be assumed to be valid after the return
        // of this method. Until https://github.com/jpy-consortium/jpy/issues/126 is resolved, we need to make a copy of
        // the header to use after the return of this method.
        ByteBuffer original = mi.header.getByteBuffer();
        ByteBuffer copy = ByteBuffer.allocate(original.remaining()).put(original).rewind();
        Schema schema = new Schema();
        Message.getRootAsMessage(copy).header(schema);

        return schema;
    }

    public static PrimitiveIterator.OfLong extractBufferInfo(@NotNull final RecordBatch batch) {
        final long[] bufferInfo = new long[batch.buffersLength()];
        for (int bi = 0; bi < batch.buffersLength(); ++bi) {
            int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).offset());
            int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).length());

            if (bi < batch.buffersLength() - 1) {
                final int nextOffset =
                        LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi + 1).offset());
                // our parsers handle overhanging buffers
                length += Math.max(0, nextOffset - offset - length);
            }
            bufferInfo[bi] = length;
        }
        return Arrays.stream(bufferInfo).iterator();
    }

    @ScriptApi
    public synchronized void setSchema(final ByteBuffer ipcMessage) {
        // The input ByteBuffer instance (especially originated from Python) can't be assumed to be valid after the
        // return of this method. Until https://github.com/jpy-consortium/jpy/issues/126 is resolved, we need to copy
        // the data out of the input ByteBuffer to use after the return of this method.
        if (completed) {
            throw new IllegalStateException("Conversion is complete; cannot process additional messages");
        }
        final BarrageProtoUtil.MessageInfo mi = parseArrowIpcMessage(ipcMessage);
        configureWithSchema(parseArrowSchema(mi));
    }

    @ScriptApi
    public synchronized void addRecordBatches(final ByteBuffer... ipcMessages) {
        // The input ByteBuffer instance (especially originated from Python) can't be assumed to be valid after the
        // return of this method. Until https://github.com/jpy-consortium/jpy/issues/126 is resolved, we need to copy
        // the data out of the input ByteBuffer to use after the return of this method.
        for (final ByteBuffer ipcMessage : ipcMessages) {
            addRecordBatch(ipcMessage);
        }
    }

    @ScriptApi
    public synchronized void addRecordBatch(final ByteBuffer ipcMessage) {
        // The input ByteBuffer instance (especially originated from Python) can't be assumed to be valid after the
        // return of this method. Until https://github.com/jpy-consortium/jpy/issues/126 is resolved, we need to copy
        // the data out of the input ByteBuffer to use after the return of this method.
        if (completed) {
            throw new IllegalStateException("Conversion is complete; cannot process additional messages");
        }
        if (resultTable == null) {
            throw new IllegalStateException("Arrow schema must be provided before record batches can be added");
        }

        final BarrageProtoUtil.MessageInfo mi = parseArrowIpcMessage(ipcMessage);
        if (mi.header.headerType() != MessageHeader.RecordBatch) {
            throw new IllegalArgumentException("The input is not a valid Arrow RecordBatch IPC message");
        }

        final int numColumns = resultTable.getColumnSources().size();
        BarrageMessage msg = createBarrageMessage(mi, numColumns);
        msg.rowsAdded = RowSetFactory.fromRange(totalRowsRead, totalRowsRead + msg.length - 1);
        msg.rowsIncluded = msg.rowsAdded.copy();
        msg.modColumnData = BarrageMessage.ZERO_MOD_COLUMNS;
        totalRowsRead += msg.length;
        resultTable.handleBarrageMessage(msg);
    }

    @ScriptApi
    public synchronized BarrageTable getResultTable() {
        if (!completed) {
            throw new IllegalStateException("Conversion must be completed prior to requesting the result");
        }
        return resultTable;
    }

    @ScriptApi
    public synchronized void onCompleted() throws InterruptedException {
        if (completed) {
            throw new IllegalStateException("Conversion cannot be completed twice");
        }
        completed = true;
    }

    protected void configureWithSchema(final Schema schema) {
        if (resultTable != null) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Schema evolution not supported");
        }

        final BarrageUtil.ConvertedArrowSchema result = BarrageUtil.convertArrowSchema(schema);
        resultTable = BarrageTable.make(null, result.tableDef, result.attributes, true, null);
        resultTable.setFlat();

        ChunkType[] columnChunkTypes = result.computeWireChunkTypes();
        columnTypes = result.computeWireTypes();
        componentTypes = result.computeWireComponentTypes();
        for (int i = 0; i < schema.fieldsLength(); i++) {
            final int factor = (result.conversionFactors == null) ? 1 : result.conversionFactors[i];
            ChunkReader reader = DefaultChunkReadingFactory.INSTANCE.getReader(options, factor,
                    typeInfo(columnChunkTypes[i], columnTypes[i], componentTypes[i], schema.fields(i)));
            readers.add(reader);
        }

        // retain reference until the resultTable can be sealed
        resultTable.retainReference();
    }

    protected BarrageMessage createBarrageMessage(BarrageProtoUtil.MessageInfo mi, int numColumns) {
        // The BarrageProtoUtil.MessageInfo instance (especially originated from Python) can't be assumed to be valid
        // after the return of this method. Until https://github.com/jpy-consortium/jpy/issues/126 is resolved, we need
        // to make a copy of it to use after the return of this method.
        final BarrageMessage msg = new BarrageMessage();
        final RecordBatch batch = (RecordBatch) mi.header.header(new RecordBatch());

        final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                        i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

        final PrimitiveIterator.OfLong bufferInfoIter = extractBufferInfo(batch);

        msg.rowsRemoved = RowSetFactory.empty();
        msg.shifted = RowSetShiftData.EMPTY;

        // include all columns as add-columns
        int numRowsAdded = LongSizedDataStructure.intSize("RecordBatch.length()", batch.length());
        msg.addColumnData = new BarrageMessage.AddColumnData[numColumns];
        for (int ci = 0; ci < numColumns; ++ci) {
            final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
            msg.addColumnData[ci] = acd;
            msg.addColumnData[ci].data = new ArrayList<>();
            try {
                acd.data.add(readers.get(ci).readChunk(fieldNodeIter, bufferInfoIter, mi.inputStream, null, 0, 0));
            } catch (final IOException unexpected) {
                throw new UncheckedDeephavenException(unexpected);
            }

            if (acd.data.get(0).size() != numRowsAdded) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Inconsistent num records per column: " + numRowsAdded + " != " + acd.data.get(0).size());
            }
            acd.type = columnTypes[ci];
            acd.componentType = componentTypes[ci];
        }

        msg.length = numRowsAdded;
        return msg;
    }
}
