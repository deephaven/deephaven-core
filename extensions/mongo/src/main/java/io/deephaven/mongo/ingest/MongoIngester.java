/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.mongo.ingest;

import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.io.logger.Logger;
import io.deephaven.qst.type.Type;
import io.deephaven.streampublisher.KeyOrValueIngestData;
import io.deephaven.streampublisher.KeyOrValueSpec;
import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.commons.lang3.mutable.MutableInt;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * An ingester that consumes a Mongo change stream.
 *
 * <p>
 * This class is an internal implementation detail for io.deephaven.mongo; is not intended to be used directly by client
 * code. It lives in a separate package as a means of code organization.
 */
@InternalUseOnly
public class MongoIngester {
    private static final int REPORT_INTERVAL_MS = Configuration.getInstance().getIntegerForClassWithDefault(
            MongoIngester.class, "reportIntervalMs", 60_000);

    private final Logger log;
    private final String uri;
    private final String databaseName;
    private final String collectionName;
    private final List<Bson> pipeline;
    private final BsonDocument resumeDocument;
    private final String name;
    private final String logPrefix;
    private final TableDefinition tableDefinition;

    private long messagesProcessed = 0;
    private long bytesProcessed = 0;
    private long lastMessages = 0;
    private long lastBytes = 0;

    private volatile boolean done;

    private final MongoStreamPublisher streamPublisher;


    public MongoIngester(
            @NotNull final Logger log,
            @NotNull MongoChangeStreamParameters parameters,
            @Nullable final String resumeTokenData) {
        this.log = log;
        this.uri = parameters.uri();
        if (resumeTokenData != null) {
            this.resumeDocument = new BsonDocument("_data", new BsonString(resumeTokenData));
        } else {
            this.resumeDocument = null;
        }
        this.databaseName = parameters.database();
        this.collectionName = parameters.collection();
        name = String.format("%s(%s, %s)", MongoIngester.class.getSimpleName(), parameters.database(),
                parameters.collection());
        logPrefix = name + ": ";
        this.pipeline = parameters.pipeline();

        final List<ColumnDefinition<?>> columnDefinitionList = new ArrayList<>();
        if (parameters.receiveTimeColumnName() != null) {
            columnDefinitionList.add(ColumnDefinition.of(parameters.receiveTimeColumnName(), Type.instantType()));
        }
        if (parameters.documentKeyColumnName() != null) {
            columnDefinitionList.add(ColumnDefinition.of(parameters.documentKeyColumnName(), Type.stringType()));
        }
        columnDefinitionList.add(ColumnDefinition.of(parameters.operationTypeColumnName(), Type.stringType()));
        if (parameters.clusterTimeColumnName() != null) {
            columnDefinitionList.add(ColumnDefinition.of(parameters.clusterTimeColumnName(), Type.instantType()));
        }
        if (parameters.clusterTimeIncrementColumnName() != null) {
            columnDefinitionList.add(ColumnDefinition.of(parameters.clusterTimeIncrementColumnName(), Type.intType()));
        }
        if (parameters.resumeFromColumnName() != null) {
            columnDefinitionList.add(ColumnDefinition.of(parameters.resumeFromColumnName(), Type.stringType()));
        }
        if (parameters.documentSizeColumnName() != null) {
            columnDefinitionList.add(ColumnDefinition.of(parameters.documentSizeColumnName(), Type.intType()));
        }
        if (parameters.documentColumnName() != null) {
            columnDefinitionList.add(ColumnDefinition.of(parameters.documentColumnName(), Type.find(byte[].class)));
        }

        final MutableInt nextColumnIndex = new MutableInt(columnDefinitionList.size());

        final KeyOrValueIngestData ingestData = parameters.documentSpec().getIngestData(KeyOrValueSpec.KeyOrValue.VALUE, null, nextColumnIndex, columnDefinitionList);

        tableDefinition = TableDefinition.of(columnDefinitionList);
        streamPublisher = new MongoStreamPublisher(log, logPrefix, tableDefinition, this::shutdown, parameters, ingestData);
    }

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    public MongoStreamPublisher getStreamPublisher() {
        return streamPublisher;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Starts a consumer thread which replicates the consumed Kafka messages to Deephaven.
     * <p>
     * This method must not be called more than once on an ingester instance.
     */
    public void start() {
        final Thread t = new Thread(this::consumerLoop, this.toString());
        t.setDaemon(true);
        t.start();
    }

    private static double unitsPerSec(final long units, final long nanos) {
        if (nanos <= 0) {
            return 0;
        }
        return 1000.0 * 1000.0 * 1000.0 * units / nanos;
    }

    private void consumerLoop() {
        final long reportIntervalNanos = REPORT_INTERVAL_MS * 1_000_000L;
        long lastReportNanos = System.nanoTime();
        long nextReport = lastReportNanos + reportIntervalNanos;
        final DecimalFormat rateFormat = new DecimalFormat("#.###");

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            final MongoDatabase db = mongoClient.getDatabase(databaseName);
            final MongoCollection<Document> collection = db.getCollection(collectionName);

            ChangeStreamIterable<RawBsonDocument> watcher;
            if (pipeline != null) {
                watcher = collection.watch(pipeline, RawBsonDocument.class);
            } else {
                watcher = collection.watch(RawBsonDocument.class);
            }

            watcher = watcher.fullDocument(FullDocument.UPDATE_LOOKUP);
            if (resumeDocument != null) {
                watcher.resumeAfter(resumeDocument);
            }

            try (MongoCursor<ChangeStreamDocument<RawBsonDocument>> cursor = watcher.iterator()) {
                while (!done && cursor.hasNext()) {
                    final ChangeStreamDocument<RawBsonDocument> nv = cursor.next();
                    ++messagesProcessed;
                    bytesProcessed += streamPublisher.processDocument(nv);

                    final long afterPoll = System.nanoTime();
                    if (afterPoll > nextReport) {
                        final long periodMessages = messagesProcessed - lastMessages;
                        final long periodBytes = bytesProcessed - lastBytes;
                        final long periodNanos = afterPoll - lastReportNanos;
                        log.info().append(logPrefix)
                                .append("ingestion period summary")
                                .append(", messages=").append(periodMessages)
                                .append(", bytes=").append(periodBytes)
                                .append(", time=").append(periodNanos / 1000_000L).append("ms")
                                .append(", msgs/sec=")
                                .append(rateFormat.format(unitsPerSec(periodMessages, periodNanos)))
                                .append(", bytes/sec=").append(rateFormat.format(unitsPerSec(periodBytes, periodNanos)))
                                .endl();
                        lastReportNanos = afterPoll;
                        nextReport = lastReportNanos + reportIntervalNanos;
                        lastMessages = messagesProcessed;
                        lastBytes = bytesProcessed;
                    }
                }
            }
        } catch (Exception e) {
            log.info().append(logPrefix).append("Caught exception while processing change stream: ").append(e).endl();
            streamPublisher.propagateFailure(e);
        } finally {
            log.info().append(logPrefix).append("Closing MongoClient").endl();
        }
    }

    public void shutdown() {
        if (done) {
            return;
        }
        // synchronized (streamConsumers) {
        // streamConsumers.clear();
        // }
        done = true;
        // need to figure out how to wakeup our consumer, we probably should convert to async
        // kafkaConsumer.wakeup();
    }
}
