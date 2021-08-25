package io.deephaven.db.v2.tuples;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.tuples.TupleSourceCodeGenerator.ColumnSourceType;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Factory for {@link TupleSource} instances.
 */
public class TupleSourceFactory {

    private static final Map<Class, ColumnSourceType> EXPLICIT_CLASS_TO_COLUMN_SOURCE_TYPE =
        Collections.unmodifiableMap(Arrays.stream(ColumnSourceType.values())
            .filter(et -> et != ColumnSourceType.OBJECT && !et.isReinterpreted())
            .collect(Collectors.toMap(ColumnSourceType::getElementClass, Function.identity())));

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
            // NB: Don't reinterpret here, or you may have a bad time with join states when the LHS
            // and RHS columns are differently reinterpretable.
            return columnSources[0];
        }
        if (length < 4) {
            // NB: The array copy that looks like a side effect here is in fact deliberate and
            // desirable.
            final ColumnSourceType types[] = new ColumnSourceType[length];
            final ColumnSource internalSources[] = new ColumnSource[length];
            for (int csi = 0; csi < length; ++csi) {
                types[csi] = getColumnSourceType(columnSources[csi]);
                internalSources[csi] = maybeReinterpret(types[csi], columnSources[csi]);
            }
            final String factoryClassName = TupleSourceCodeGenerator.generateClassName(types);
            final Class<TupleSource> factoryClass;
            try {
                // noinspection unchecked
                factoryClass = (Class<TupleSource>) Class.forName(factoryClassName);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(
                    "Could not find tuple factory class for name " + factoryClassName, e);
            }
            final Constructor<TupleSource> factoryConstructor;
            try {
                factoryConstructor = length == 2
                    ? factoryClass.getConstructor(ColumnSource.class, ColumnSource.class)
                    : factoryClass.getConstructor(ColumnSource.class, ColumnSource.class,
                        ColumnSource.class);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException(
                    "Could not find tuple factory constructor for name " + factoryClassName, e);
            }
            try {
                return factoryConstructor.newInstance((Object[]) internalSources);
            } catch (InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
                throw new IllegalStateException("Could not construct " + factoryClassName, e);
            }
        }
        // NB: Don't reinterpret here, or you may have a bad time with join states when the LHS and
        // RHS columns are differently reinterpretable.
        return new MultiColumnTupleSource(
            Arrays.stream(columnSources).toArray(ColumnSource[]::new));
    }

    private static ColumnSourceType getColumnSourceType(@NotNull final ColumnSource columnSource) {
        final ColumnSourceType candidate = EXPLICIT_CLASS_TO_COLUMN_SOURCE_TYPE
            .getOrDefault(columnSource.getType(), ColumnSourceType.OBJECT);
        if (candidate.getReinterpretAsType() != null
            && columnSource.allowsReinterpret(candidate.getInternalClass())) {
            return candidate.getReinterpretAsType();
        }
        return candidate;
    }

    private static ColumnSource maybeReinterpret(@NotNull final ColumnSourceType type,
        @NotNull final ColumnSource columnSource) {
        return type.isReinterpreted() ? columnSource.reinterpret(type.getElementClass())
            : columnSource;
    }
}
