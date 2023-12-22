package io.deephaven.mongo;

import de.undercouch.bson4jackson.BsonFactory;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.mongo.ingest.MongoChangeStreamParameters;
import io.deephaven.mongo.ingest.MongoIngester;
import io.deephaven.mongo.ingest.MongoStreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.streampublisher.BlinkTableOperation;
import io.deephaven.streampublisher.KeyOrValueSpec;
import io.deephaven.streampublisher.json.JsonConsumeImpl;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

public class MongoReplicator {
    private static final Logger log = LoggerFactory.getLogger(MongoReplicator.class);

    @ScriptApi
    public static Table replicateToTable(MongoChangeStreamParameters parameters) {
        return replicateToTable(parameters, null);
    }

    @ScriptApi
    public static Table replicateToTable(MongoChangeStreamParameters parameters, @Nullable String resumeTokenData) {
        final MongoIngester ingester = new MongoIngester(log, parameters, resumeTokenData);
        final TableDefinition td = ingester.getTableDefinition();
        final MongoStreamPublisher sp = ingester.getStreamPublisher();


        // StreamToBlinkTableAdapter registers itself in its constructor
        // noinspection resource
        final StreamToBlinkTableAdapter streamToBlinkTableAdapter = new StreamToBlinkTableAdapter(
                td,
                sp,
                ExecutionContext.getContext().getUpdateGraph(),
                "Kafka-" + parameters.database() + '-' + parameters.collection());
        final Table blinkTable = streamToBlinkTableAdapter.table();
        final Table result = parameters.tableType().walk(new BlinkTableOperation(blinkTable));

        ingester.start();

        return result;
    }

    public static KeyOrValueSpec jsonSpec(
            @NotNull final ColumnDefinition<?>[] columnDefinitions,
            @Nullable final Map<String, String> fieldToColumnName) {
        return new JsonConsumeImpl(columnDefinitions, fieldToColumnName, new com.fasterxml.jackson.databind.ObjectMapper(new BsonFactory()));
    }

    public static KeyOrValueSpec jsonSpec(
            @NotNull final ColumnDefinition<?>[] columnDefinitions) {
        return new JsonConsumeImpl(columnDefinitions, null, new com.fasterxml.jackson.databind.ObjectMapper(new BsonFactory()));
    }

    public static KeyOrValueSpec jsonSpec(
            @NotNull final Collection<ColumnDefinition<?>> columnDefinitions) {
        return new JsonConsumeImpl(columnDefinitions.toArray(ColumnDefinition[]::new), null, new com.fasterxml.jackson.databind.ObjectMapper(new BsonFactory()));
    }
}
