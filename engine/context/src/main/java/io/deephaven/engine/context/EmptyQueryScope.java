//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.engine.liveness.LivenessReferent;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class EmptyQueryScope implements QueryScope {
    public final static EmptyQueryScope INSTANCE = new EmptyQueryScope();

    private EmptyQueryScope() {}

    @Override
    public Set<String> getParamNames() {
        return new HashSet<>();
    }

    @Override
    public boolean hasParamName(String name) {
        return false;
    }

    @Override
    public <T> QueryScopeParam<T> createParam(String name) throws MissingVariableException {
        throw new MissingVariableException("Missing variable " + name);
    }

    @Override
    public <T> T readParamValue(String name) throws MissingVariableException {
        throw new MissingVariableException("Missing variable " + name);
    }

    @Override
    public <T> T readParamValue(String name, T defaultValue) {
        return defaultValue;
    }

    @Override
    public <T> void putParam(String name, T value) {
        throw new IllegalStateException("EmptyQueryScope cannot create parameters");
    }

    @Override
    public Map<String, Object> toMap(@NotNull ParamFilter<Object> filter) {
        return new HashMap<>();
    }

    @Override
    public <T> Map<String, T> toMap(@NotNull Function<Object, T> valueMapper, @NotNull ParamFilter<T> filter) {
        return new HashMap<>();
    }

    @Override
    public boolean tryManage(@NotNull LivenessReferent referent) {
        throw new UnsupportedOperationException("tryManage");
    }

    @Override
    public boolean tryUnmanage(@NotNull LivenessReferent referent) {
        throw new UnsupportedOperationException("tryUnmanage");
    }

    @Override
    public boolean tryUnmanage(@NotNull Stream<? extends LivenessReferent> referents) {
        throw new UnsupportedOperationException("tryUnmanage");
    }

    @Override
    public boolean tryRetainReference() {
        throw new UnsupportedOperationException("tryRetainReference");
    }

    @Override
    public void dropReference() {
        throw new UnsupportedOperationException("dropReference");
    }

    @Override
    public WeakReference<? extends LivenessReferent> getWeakReference() {
        throw new UnsupportedOperationException("getWeakReference");
    }
}
