//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Map-based implementation, extending LivenessArtifact to manage the objects passed into it.
 */
public class StandaloneQueryScope extends LivenessArtifact implements QueryScope {

    private final Map<String, ValueRetriever<?>> valueRetrievers =
            new KeyedObjectHashMap<>(new ValueRetrieverNameKey());

    @Override
    public Set<String> getParamNames() {
        return new HashSet<>(valueRetrievers.keySet());
    }

    @Override
    public boolean hasParamName(String name) {
        return valueRetrievers.containsKey(name);
    }

    @Override
    public <T> QueryScopeParam<T> createParam(final String name) throws MissingVariableException {
        final ValueRetriever<?> valueRetriever = valueRetrievers.get(name);
        if (valueRetriever == null) {
            throw new MissingVariableException("Missing variable " + name);
        }
        // noinspection unchecked
        return (QueryScopeParam<T>) valueRetriever.createParam();
    }

    @Override
    public <T> T readParamValue(final String name) throws MissingVariableException {
        final ValueRetriever<?> valueRetriever = valueRetrievers.get(name);
        if (valueRetriever == null) {
            throw new MissingVariableException("Missing variable " + name);
        }
        // noinspection unchecked
        return (T) valueRetriever.getValue();
    }

    @Override
    public <T> T readParamValue(final String name, final T defaultValue) {
        final ValueRetriever<?> valueRetriever = valueRetrievers.get(name);
        if (valueRetriever == null) {
            return defaultValue;
        }
        // noinspection unchecked
        return (T) valueRetriever.getValue();
    }

    @Override
    public <T> void putParam(final String name, final T value) {
        if (value instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(value)) {
            manage((LivenessReferent) value);
        }
        ValueRetriever<?> oldValueRetriever =
                valueRetrievers.put(name, new ValueRetriever<>(name, (Object) value));

        if (oldValueRetriever != null) {
            Object oldValue = oldValueRetriever.getValue();
            if (oldValue instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(oldValue)) {
                unmanage((LivenessReferent) oldValue);
            }
        }
    }

    @Override
    public Map<String, Object> toMap(@NotNull final ParamFilter<Object> filter) {
        return toMapInternal(null, filter);
    }

    @Override
    public <T> Map<String, T> toMap(
            @NotNull final Function<Object, T> valueMapper,
            @NotNull final ParamFilter<T> filter) {
        return toMapInternal(valueMapper, filter);
    }

    private <T> Map<String, T> toMapInternal(
            @Nullable final Function<Object, T> valueMapper,
            @NotNull final ParamFilter<T> filter) {
        final Map<String, T> result = new HashMap<>();

        for (final Map.Entry<String, ValueRetriever<?>> entry : valueRetrievers.entrySet()) {
            final String name = entry.getKey();
            final ValueRetriever<?> valueRetriever = entry.getValue();
            Object value = valueRetriever.getValue();
            if (valueMapper != null) {
                value = valueMapper.apply(value);
            }

            // noinspection unchecked
            if (filter.accept(name, (T) value)) {
                // noinspection unchecked
                result.put(name, (T) value);
            }
        }

        return result;
    }

    private static class ValueRetriever<T> {

        protected final T value;
        private final String name;

        protected ValueRetriever(String name, final T value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public T getValue() {
            return value;
        }

        public QueryScopeParam<T> createParam() {
            return new QueryScopeParam<>(getName(), getValue());
        }
    }

    private static class ValueRetrieverNameKey extends KeyedObjectKey.Basic<String, ValueRetriever<?>> {
        @Override
        public String getKey(ValueRetriever valueRetriever) {
            return valueRetriever.getName();
        }
    }
}
