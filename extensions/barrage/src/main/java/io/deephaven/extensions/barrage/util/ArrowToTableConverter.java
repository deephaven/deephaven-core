/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.util;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.rpc.Code;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.io.streams.ByteBufferInputStream;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;

import static io.deephaven.extensions.barrage.util.BarrageProtoUtil.DEFAULT_SER_OPTIONS;

/**
 * This class allows the incremental making of a BarrageTable from Arrow IPC messages, starting with an Arrow Schema
 * message followed by zero or more RecordBatches
 */
public class ArrowToTableConverter {
    protected long totalRowsRead = 0;
    protected BarrageTable resultTable;
    private ChunkType[] columnChunkTypes;
    private int[] columnConversionFactors;
    private Class<?>[] columnTypes;
    private Class<?>[] componentTypes;
    protected BarrageSubscriptionOptions options = DEFAULT_SER_OPTIONS;

    private volatile boolean completed = false;

    private static BarrageProtoUtil.MessageInfo parseArrowIpcMessage(final byte[] ipcMessage) throws IOException {
        final BarrageProtoUtil.MessageInfo mi = new BarrageProtoUtil.MessageInfo();

        final ByteBuffer bb = ByteBuffer.wrap(ipcMessage);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        final int continuation = bb.getInt();
        final int metadata_size = bb.getInt();
        mi.header = Message.getRootAsMessage(bb);

        if (mi.header.headerType() == MessageHeader.RecordBatch) {
            bb.position(metadata_size + 8);
            final ByteBuffer bodyBB = bb.slice();
            final ByteBufferInputStream bbis = new ByteBufferInputStream(bodyBB);
            final CodedInputStream decoder = CodedInputStream.newInstance(bbis);
            // noinspection UnstableApiUsage
            mi.inputStream = new LittleEndianDataInputStream(
                    new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, bodyBB.remaining()));
        }
        return mi;
    }

    @ScriptApi
    public synchronized void setSchema(final byte[] ipcMessage) {
        if (completed) {
            throw new IllegalStateException("Conversion is complete; cannot process additional messages");
        }
        final BarrageProtoUtil.MessageInfo mi = getMessageInfo(ipcMessage);
        if (mi.header.headerType() != MessageHeader.Schema) {
            throw new IllegalArgumentException("The input is not a valid Arrow Schema IPC message");
        }
        parseSchema((Schema) mi.header.header(new Schema()));
    }

    @ScriptApi
    public synchronized void addRecordBatch(final byte[] ipcMessage) {
        if (completed) {
            throw new IllegalStateException("Conversion is complete; cannot process additional messages");
        }
        if (resultTable == null) {
            throw new IllegalStateException("Arrow schema must be provided before record batches can be added");
        }

        final BarrageProtoUtil.MessageInfo mi = getMessageInfo(ipcMessage);
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

    protected void parseSchema(final Schema header) {
        if (resultTable != null) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Schema evolution not supported");
        }

        final BarrageUtil.ConvertedArrowSchema result = BarrageUtil.convertArrowSchema(header);
        resultTable = BarrageTable.make(null, result.tableDef, result.attributes, null);
        resultTable.setFlat();

        columnConversionFactors = result.conversionFactors;
        columnChunkTypes = result.computeWireChunkTypes();
        columnTypes = result.computeWireTypes();
        componentTypes = result.computeWireComponentTypes();

        // retain reference until the resultTable can be sealed
        resultTable.retainReference();
    }

    protected BarrageMessage createBarrageMessage(BarrageProtoUtil.MessageInfo mi, int numColumns) {
        final BarrageMessage msg = new BarrageMessage();
        final RecordBatch batch = (RecordBatch) mi.header.header(new RecordBatch());

        final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                        i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

        final TLongArrayList bufferInfo = new TLongArrayList(batch.buffersLength());
        for (int bi = 0; bi < batch.buffersLength(); ++bi) {
            int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).offset());
            int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).length());

            if (bi < batch.buffersLength() - 1) {
                final int nextOffset =
                        LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi + 1).offset());
                // our parsers handle overhanging buffers
                length += Math.max(0, nextOffset - offset - length);
            }
            bufferInfo.add(length);
        }
        final TLongIterator bufferInfoIter = bufferInfo.iterator();

        msg.rowsRemoved = RowSetFactory.empty();
        msg.shifted = RowSetShiftData.EMPTY;

        // include all columns as add-columns
        int numRowsAdded = LongSizedDataStructure.intSize("RecordBatch.length()", batch.length());
        msg.addColumnData = new BarrageMessage.AddColumnData[numColumns];
        for (int ci = 0; ci < numColumns; ++ci) {
            final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
            msg.addColumnData[ci] = acd;
            msg.addColumnData[ci].data = new ArrayList<>();
            final int factor = (columnConversionFactors == null) ? 1 : columnConversionFactors[ci];
            try {
                acd.data.add(ChunkInputStreamGenerator.extractChunkFromInputStream(options, factor,
                        columnChunkTypes[ci], columnTypes[ci], componentTypes[ci], fieldNodeIter,
                        bufferInfoIter, mi.inputStream, null, 0, 0));
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

    private BarrageProtoUtil.MessageInfo getMessageInfo(byte[] ipcMessage) {
        final BarrageProtoUtil.MessageInfo mi;
        try {
            mi = parseArrowIpcMessage(ipcMessage);
        } catch (IOException unexpected) {
            throw new UncheckedDeephavenException(unexpected);
        }
        return mi;
    }
}
