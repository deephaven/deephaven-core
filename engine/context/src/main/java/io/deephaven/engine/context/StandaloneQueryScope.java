package io.deephaven.engine.context;

import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Map-based implementation, extending LivenessArtifact to manage the objects passed into it.
 */
public class StandaloneQueryScope extends LivenessArtifact implements QueryScope {

    private final KeyedObjectHashMap<String, ValueRetriever<?>> valueRetrievers =
            new KeyedObjectHashMap<>(new ValueRetrieverNameKey());

    @Override
    public Set<String> getParamNames() {
        return valueRetrievers.keySet();
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
        NameValidator.validateQueryParameterName(name);
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
    public Map<String, Object> toMap(@NotNull final Predicate<Map.Entry<String, Object>> predicate) {
        final HashMap<String, Object> result = new HashMap<>();
        valueRetrievers.entrySet().stream()
                .map(e -> ImmutablePair.of(e.getKey(), (Object) e.getValue().value))
                .filter(predicate)
                .forEach(e -> result.put(e.getKey(), e.getValue()));
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
