package io.deephaven.db.v2.select;

import io.deephaven.base.Pair;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.chunk.Chunk;

import java.util.concurrent.ConcurrentHashMap;

public interface ArgumentsBuilder<CONTEXT extends ArgumentsBuilder.Context> {
    static ConcurrentHashMap<Pair<Class,String>,ArgumentsBuilder> builderCache = new ConcurrentHashMap<>();

    static ArgumentsBuilder getBuilder(Class usedColumnsType, String npTypeName) {
        return builderCache.computeIfAbsent(new Pair<>(usedColumnsType,npTypeName),k -> {
            if (usedColumnsType.isPrimitive() || usedColumnsType == Boolean.class) {
                return new PrimitiveArgumentsBuilder(npTypeName,usedColumnsType);
            } else if (usedColumnsType.isArray() && usedColumnsType.getComponentType().isPrimitive()) {
                return new PrimitiveArrayArgumentsBuilder(npTypeName);
            } else if (usedColumnsType == String.class) {
                return new StringArgumentsBuilder();
            } else if (DbArrayBase.class.isAssignableFrom(usedColumnsType)) {
                return new DbArrayArgumentsBuilder(npTypeName);
            }
            throw new RuntimeException("Arguments of type " + usedColumnsType + " not supported");
        });
    }

    interface Context {}

    int getArgumentsCount();
    Object[] packArguments(Chunk incomingData, CONTEXT context, int size);

    CONTEXT getContext();

}
