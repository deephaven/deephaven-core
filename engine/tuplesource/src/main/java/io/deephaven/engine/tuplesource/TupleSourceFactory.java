package io.deephaven.engine.tuplesource;

import io.deephaven.base.Pair;
import io.deephaven.engine.chunk.ChunkType;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.time.DateTime;
import io.deephaven.engine.v2.sources.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Factory for {@link TupleSource} instances.
 */
public class TupleSourceFactory {

    /**
     * Create a {@link TupleSource} for the supplied array of {@link ColumnSource}s.
     *
     * @param columnSources The column sources
     * @return The tuple factory
     */
    public static TupleSource makeTupleSource(@NotNull final ColumnSource... columnSources) {
        final int length = columnSources.length;
        if (length == 0) {
            return EmptyTupleSource.INSTANCE;
        }
        if (length == 1) {
            // NB: Don't reinterpret here, or you may have a bad time with join states when the LHS and RHS columns are
            // differently reinterpretable.
            return columnSources[0];
        }
        if (length < 4) {
            // NB: The array copy that looks like a side effect here is in fact deliberate and desirable.
            //noinspection unchecked
            final Pair<String, ColumnSource>[] typesNamesAndInternalSources = new Pair[length];
            for (int csi = 0; csi < length; ++csi) {
                typesNamesAndInternalSources[csi] = columnSourceToTypeNameAndInternalSource(columnSources[csi]);
            }
            final String factoryClassName = Stream.of(typesNamesAndInternalSources)
                    .map(Pair::getFirst).collect(Collectors.joining()) + "ColumnTupleSource";
            final Class<TupleSource> factoryClass;
            try {
                // noinspection unchecked
                factoryClass = (Class<TupleSource>) Class.forName(factoryClassName);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Could not find tuple factory class for name " + factoryClassName, e);
            }
            final Constructor<TupleSource> factoryConstructor;
            try {
                factoryConstructor = length == 2
                        ? factoryClass.getConstructor(ColumnSource.class, ColumnSource.class)
                        : factoryClass.getConstructor(ColumnSource.class, ColumnSource.class, ColumnSource.class);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("Could not find tuple factory constructor for name " + factoryClassName,
                        e);
            }
            try {
                return factoryConstructor.newInstance((Object[]) Stream.of(typesNamesAndInternalSources)
                        .map(Pair::getSecond).toArray(ColumnSource[]::new));
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Could not construct " + factoryClassName, e);
            }
        }
        // NB: Don't reinterpret here, or you may have a bad time with join states when the LHS and RHS columns are
        // differently reinterpretable.
        return new MultiColumnTupleSource(Arrays.stream(columnSources).toArray(ColumnSource[]::new));
    }

    private static final Map<Class<?>, Class<?>> TYPE_TO_REINTERPRETED =
            Map.of(Boolean.class, byte.class, DateTime.class, long.class);

    private static Pair<String, ColumnSource> columnSourceToTypeNameAndInternalSource(@NotNull final ColumnSource columnSource) {
        final ChunkType chunkType = columnSource.getChunkType();
        if (chunkType != ChunkType.Object) {
            return new Pair<>(chunkType.name(), columnSource);
        }
        final Class<?> reinterpretAs = TYPE_TO_REINTERPRETED.get(columnSource.getType());
        if (reinterpretAs == null) {
            return new Pair<>(chunkType.name(), columnSource);
        }
        //noinspection unchecked
        if (columnSource.allowsReinterpret(reinterpretAs)) {
            //noinspection unchecked
            return new Pair<>("Reinterpreted" + columnSource.getType().getSimpleName(), columnSource.reinterpret(reinterpretAs));
        }
        return new Pair<>(columnSource.getType().getSimpleName(), columnSource);
    }
}
