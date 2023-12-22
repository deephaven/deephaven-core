/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.mongo.ingest;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.io.logger.Logger;
import io.deephaven.streampublisher.StreamPublisherBase;
import io.deephaven.time.DateTimeUtils;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * An adapter that maps keys and values, possibly each with multiple fields, to single Deephaven columns. Each Kafka
 * record produces one Deephaven row.
 */
public class MongoStreamPublisher extends StreamPublisherBase {

    public static final int NULL_COLUMN_INDEX = -1;

    private final Logger log;
    private final String logPrefix;
    private final Runnable shutdownCallback;
    private final MongoChangeStreamParameters parameters;
    private final int receiveTimeColumnIndex;
    private final int documentKeyColumnIndex;
    private final int operationTypeColumnIndex;
    private final int resumeFromColumnIndex;
    private final int clusterTimeColumnIndex;
    private final int clusterTimeIncrementColumnIndex;
    private final int documentColumnIndex;
    private final int documentSizeColumnIndex;

    MongoStreamPublisher(
            @NotNull final Logger log,
            @NotNull final String logPrefix,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Runnable shutdownCallback,
            @NotNull final MongoChangeStreamParameters parameters) {
        super(tableDefinition);
        this.log = log;
        this.logPrefix = logPrefix;
        this.shutdownCallback = shutdownCallback;
        this.parameters = parameters;
        receiveTimeColumnIndex = indexOf(tableDefinition, parameters.receiveTimeColumnName());
        documentKeyColumnIndex = indexOf(tableDefinition, parameters.documentKeyColumnName());
        operationTypeColumnIndex = indexOf(tableDefinition, parameters.operationTypeColumnName());
        clusterTimeColumnIndex = indexOf(tableDefinition, parameters.clusterTimeColumnName());
        clusterTimeIncrementColumnIndex = indexOf(tableDefinition, parameters.clusterTimeIncrementColumnName());
        resumeFromColumnIndex = indexOf(tableDefinition, parameters.resumeFromColumnName());
        documentSizeColumnIndex = indexOf(tableDefinition, parameters.documentSizeColumnName());
        documentColumnIndex = indexOf(tableDefinition, parameters.documentColumnName());
    }

    private static int indexOf(TableDefinition tableDefinition, String name) {
        if (name == null) {
            return -1;
        }
        final List<String> names = tableDefinition.getColumnNames();
        final int idx = names.indexOf(name);
        if (idx == -1) {
            throw new IllegalArgumentException("Definition does not contain " + name);
        }
        return idx;
    }

    public void propagateFailure(@NotNull final Throwable cause) {
        consumer.acceptFailure(cause);
    }

    synchronized long processDocument(ChangeStreamDocument<RawBsonDocument> nv) {
        final long receiveTime = DateTimeUtils.currentClock().currentTimeNanos();
        final BsonDocument resumeToken = nv.getResumeToken();
        final BsonTimestamp clusterTime = nv.getClusterTime();

        final String operationTypeString = nv.getOperationType().getValue();

        log.info().append(logPrefix).append("Cluster Time: ").append(clusterTime.toString()).endl();

        // we can convert this to NIO if we wanted to temporarily borrow the array, but copying it is safer for now
        final byte[] byteArray = getByteArray(nv.getFullDocument());
        final int documentBytes = byteArray.length;

        WritableChunk<Values>[] chunks = getChunksToFill();
        checkChunkSizes(chunks);

        if (receiveTimeColumnIndex >= 0) {
            chunks[receiveTimeColumnIndex].asWritableLongChunk().add(receiveTime);
        }
        if (documentKeyColumnIndex >= 0) {
            // TODO: this seems a little stupid, but I don't see how to watch the key as anything but a BsonDocument which is already parsed
            final RawBsonDocument rawKey = RawBsonDocument.parse(nv.getDocumentKey().toJson());
            final byte [] keyByteArray = getByteArray(rawKey);
            chunks[documentKeyColumnIndex].asWritableObjectChunk().add(keyByteArray);
        }
        chunks[operationTypeColumnIndex].asWritableObjectChunk().add(operationTypeString);
        if (clusterTimeColumnIndex >= 0) {
            chunks[clusterTimeColumnIndex].asWritableLongChunk().add(TimeUnit.SECONDS.toNanos(clusterTime.getTime()));
        }
        if (clusterTimeIncrementColumnIndex >= 0) {
            chunks[clusterTimeIncrementColumnIndex].asWritableIntChunk().add(clusterTime.getInc());
        }
        if (resumeFromColumnIndex >= 0) {
            // add the hex encoded "_data" field from the resume token
            chunks[resumeFromColumnIndex].asWritableObjectChunk().add(resumeToken.get("_data").asString().getValue());
        }
        if (documentSizeColumnIndex >= 0) {
            chunks[documentSizeColumnIndex].asWritableIntChunk().add(documentBytes);
        }

        chunks[documentColumnIndex].asWritableObjectChunk().add(byteArray);

        final int remaining = chunks[0].capacity() - chunks[0].size();
        if (remaining == 0) {
            flush();
        }

        return documentBytes;
    }

    @NotNull
    private byte[] getByteArray(RawBsonDocument fullDocument) {
        final ByteBuf buf = fullDocument.getByteBuffer();
        final int sz = buf.remaining();
        log.info().append(logPrefix).append("BSON Bytes: ").append(sz).endl();
        final byte [] byteArray = new byte[sz];
        buf.get(byteArray);
        if (buf.hasRemaining()) {
            throw new IllegalStateException("Did not fully consume buffer!");
        }
        return byteArray;
    }

    private void checkChunkSizes(WritableChunk<Values>[] chunks) {
        for (int cc = 1; cc < chunks.length; ++cc) {
            if (chunks[cc].size() != chunks[0].size()) {
                throw new IllegalStateException("Publisher chunks have size mismatch: "
                        + Arrays.stream(chunks).map(c -> Integer.toString(c.size())).collect(Collectors.joining(", ")));
            }
        }
    }

    @Override
    public void shutdown() {
        shutdownCallback.run();
    }
}
