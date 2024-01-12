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
 * Map-based implementation, with remote scope and object reflection support.
 */
public class StandaloneQueryScope extends LivenessArtifact implements QueryScope {

    private final KeyedObjectHashMap<String, ValueRetriever> valueRetrievers =
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

            if (stringValue.length() > 0 && stringValue.charAt(0) == '\''
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
        // noinspection unchecked
        final ValueRetriever<T> valueRetriever = valueRetrievers.get(name);
        if (valueRetriever == null) {
            throw new MissingVariableException("Missing variable " + name);
        }
        return valueRetriever.createParam();
    }

    @Override
    public <T> T readParamValue(final String name) throws MissingVariableException {
        // noinspection unchecked
        final ValueRetriever<T> valueRetriever = valueRetrievers.get(name);
        if (valueRetriever == null) {
            throw new MissingVariableException("Missing variable " + name);
        }
        return valueRetriever.getValue();
    }

    @Override
    public <T> T readParamValue(final String name, final T defaultValue) {
        // noinspection unchecked
        final ValueRetriever<T> valueRetriever = valueRetrievers.get(name);
        if (valueRetriever == null) {
            return defaultValue;
        }
        return valueRetriever.getValue();
    }

    @Override
    public <T> void putParam(final String name, final T value) {
        if (value instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(value)) {
            manage((LivenessReferent) value);
        }
        NameValidator.validateQueryParameterName(name);
        // TODO: Can I get rid of this applyValueConversions? It's too inconsistent to feel safe.
        ValueRetriever<?> oldValueRetriever =
                valueRetrievers.put(name, new SimpleValueRetriever<>(name, applyValueConversions(value)));

        if (oldValueRetriever != null) {
            Object oldValue = oldValueRetriever.getValue();
            if (oldValue instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(oldValue)) {
                unmanage((LivenessReferent) oldValue);
            }
        }
    }

    private static abstract class ValueRetriever<T> {

        private final String name;

        protected ValueRetriever(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public abstract T getValue();

        public abstract Class<T> getType();

        public abstract QueryScopeParam<T> createParam();
    }

    private static class ValueRetrieverNameKey extends KeyedObjectKey.Basic<String, ValueRetriever> {

        @Override
        public String getKey(ValueRetriever valueRetriever) {
            return valueRetriever.getName();
        }
    }

    private static class SimpleValueRetriever<T> extends ValueRetriever<T> {

        private final T value;

        public SimpleValueRetriever(final String name, final T value) {
            super(name);
            this.value = value;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public Class<T> getType() {
            // noinspection unchecked
            return (Class<T>) (value != null ? value.getClass() : Object.class);
        }

        @Override
        public QueryScopeParam<T> createParam() {
            return new QueryScopeParam<>(getName(), getValue());
        }
    }
}
