package io.deephaven.mongo.ingest;

import io.deephaven.streampublisher.TableType;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@Value.Immutable
public abstract class MongoChangeStreamParameters {

    @Value.Default
    public TableType tableType() {
        return TableType.blink();
    }

    public abstract String uri();
    public abstract String database();

    @Value.Default
    public String collection() {
        return "collection";
    }

    @NotNull
    @Value.Default
    public String operationTypeColumnName() {
        return "updateType";
    }

    @Nullable
    public abstract String receiveTimeColumnName();
    @Nullable
    public abstract String documentKeyColumnName();
    @Nullable
    public abstract String clusterTimeColumnName();
    @Nullable
    public abstract String clusterTimeIncrementColumnName();

    @NotNull
    public abstract String documentColumnName();

    @Nullable
    public abstract String resumeFromColumnName();

    @Nullable
    public abstract String documentSizeColumnName();

    public interface Builder {
        Builder uri(String uri);
        Builder database(String database);
        Builder collection(String collection);

        Builder receiveTimeColumnName(String name);
        Builder documentKeyColumnName(String name);
        Builder operationTypeColumnName(String name);
        Builder clusterTimeColumnName(String name);
        Builder clusterTimeIncrementColumnName(String name);
        Builder resumeFromColumnName(String name);
        Builder documentSizeColumnName(String name);
        Builder documentColumnName(String name);
        Builder tableType(TableType tableType);

        default Builder tableType(String typeName) {
            return tableType(TableType.friendlyNameToTableType(typeName));
        }

        MongoChangeStreamParameters build();
    }
}
