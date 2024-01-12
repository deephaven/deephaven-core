package io.deephaven.engine.context;

import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.Set;

/**
 * Map-based implementation, extending LivenessArtifact to manage the objects passed into it.
 */
public class StandaloneQueryScope extends LivenessArtifact implements QueryScope {

    private final KeyedObjectHashMap<String, ValueRetriever<?>> valueRetrievers =
            new KeyedObjectHashMap<>(new ValueRetrieverNameKey());

    /**
     * Apply conversions to certain scope variable values.
     *
     * @param value value
     * @return value, or an appropriately converted substitute.
     */
    private static Object applyValueConversions(final Object value) {
        if (value instanceof String) {
            final String stringValue = (String) value;

            if (!stringValue.isEmpty() && stringValue.charAt(0) == '\''
                    && stringValue.charAt(stringValue.length() - 1) == '\'') {
                final String datetimeString = stringValue.substring(1, stringValue.length() - 1);

                final Instant instant = DateTimeUtils.parseInstantQuiet(datetimeString);
                if (instant != null) {
                    return instant;
                }

                final long localTime = DateTimeUtils.parseDurationNanosQuiet(datetimeString);
                if (localTime != QueryConstants.NULL_LONG) {
                    return localTime;
                }

                final Period period = DateTimeUtils.parsePeriodQuiet(datetimeString);
                if (period != null) {
                    return period;
                }

                final Duration duration = DateTimeUtils.parseDurationQuiet(datetimeString);
                if (duration != null) {
                    return duration;
                }

                throw new RuntimeException("Cannot parse datetime/time/period : " + stringValue);
            }
        }

        return value;
    }

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
        //noinspection unchecked
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
        NameValidator.validateQueryParameterName(name);
        // TODO: Can I get rid of this applyValueConversions? It's too inconsistent to feel safe.
        ValueRetriever<?> oldValueRetriever =
                valueRetrievers.put(name, new ValueRetriever<>(name, applyValueConversions(value)));

        if (oldValueRetriever != null) {
            Object oldValue = oldValueRetriever.getValue();
            if (oldValue instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(oldValue)) {
                unmanage((LivenessReferent) oldValue);
            }
        }
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
