package io.deephaven.engine.table.impl.tuplesource;

import com.google.auto.service.AutoService;
import io.deephaven.base.Pair;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.time.DateTime;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Factory for {@link TupleSource} instances.
 */
public class TupleSourceCreatorImpl implements TupleSourceFactory.TupleSourceCreator {

    @AutoService(TupleSourceFactory.TupleSourceCreatorProvider.class)
    public static final class TupleSourceCreatorProvider implements TupleSourceFactory.TupleSourceCreatorProvider {

        @Override
        public TupleSourceFactory.TupleSourceCreator get() {
            return INSTANCE;
        }
    }

    private static final TupleSourceFactory.TupleSourceCreator INSTANCE = new TupleSourceCreatorImpl();

    private TupleSourceCreatorImpl() {}

    @Override
    public <TUPLE_TYPE> TupleSource<TUPLE_TYPE> makeTupleSource(@NotNull final ColumnSource... columnSources) {
        final int length = columnSources.length;
        if (length == 0) {
            // noinspection unchecked
            return (TupleSource<TUPLE_TYPE>) EmptyTupleSource.INSTANCE;
        }
        if (length == 1) {
            // NB: Don't reinterpret here, or you may have a bad time with join states when the LHS and RHS columns are
            // differently reinterpretable.
            // noinspection unchecked
            return columnSources[0];
        }
        if (length < 4) {
            // NB: The array copy that looks like a side effect here is in fact deliberate and desirable.
            // noinspection unchecked
            final Pair<String, ColumnSource>[] typesNamesAndInternalSources = new Pair[length];
            for (int csi = 0; csi < length; ++csi) {
                typesNamesAndInternalSources[csi] = columnSourceToTypeNameAndInternalSource(columnSources[csi]);
            }
            final String factoryClassName =
                    getClass().getPackageName()
                            + ".generated."
                            + Stream.of(typesNamesAndInternalSources).map(Pair::getFirst).collect(Collectors.joining())
                            + "ColumnTupleSource";
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
                // noinspection unchecked
                return factoryConstructor.newInstance((Object[]) Stream.of(typesNamesAndInternalSources)
                        .map(Pair::getSecond).toArray(ColumnSource[]::new));
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Could not construct " + factoryClassName, e);
            }
        }
        // NB: Don't reinterpret here, or you may have a bad time with join states when the LHS and RHS columns are
        // differently reinterpretable.
        // noinspection unchecked
        return (TupleSource<TUPLE_TYPE>) new MultiColumnTupleSource(
                Arrays.stream(columnSources).toArray(ColumnSource[]::new));
    }

    private static final Map<Class<?>, Class<?>> TYPE_TO_REINTERPRETED =
            Map.of(Boolean.class, byte.class, DateTime.class, long.class);

    private static Pair<String, ColumnSource> columnSourceToTypeNameAndInternalSource(
            @NotNull final ColumnSource columnSource) {
        final Class<?> dataType = columnSource.getType();
        if (dataType.isPrimitive()) {
            return new Pair<>(TypeUtils.getBoxedType(dataType).getSimpleName(), columnSource);
        }
        final Class<?> reinterpretAs = TYPE_TO_REINTERPRETED.get(columnSource.getType());
        if (reinterpretAs == null) {
            return new Pair<>("Object", columnSource);
        }
        // noinspection unchecked
        if (columnSource.allowsReinterpret(reinterpretAs)) {
            // noinspection unchecked
            return new Pair<>("Reinterpreted" + columnSource.getType().getSimpleName(),
                    columnSource.reinterpret(reinterpretAs));
        }
        return new Pair<>(columnSource.getType().getSimpleName(), columnSource);
    }
}
