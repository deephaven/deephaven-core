//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.util.ExecutionContextRegistrationException;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class PoisonedQueryScope implements QueryScope {

    public static final PoisonedQueryScope INSTANCE = new PoisonedQueryScope();

    private PoisonedQueryScope() {}

    private <T> T fail() {
        throw ExecutionContextRegistrationException.onFailedComponentAccess("QueryScope");
    }

    @Override
    public Set<String> getParamNames() {
        return fail();
    }

    @Override
    public boolean hasParamName(String name) {
        return fail();
    }

    @Override
    public <T> QueryScopeParam<T> createParam(String name) throws MissingVariableException {
        return fail();
    }

    @Override
    public <T> T readParamValue(String name) throws MissingVariableException {
        return fail();
    }

    @Override
    public <T> T readParamValue(String name, T defaultValue) {
        return fail();
    }

    @Override
    public <T> void putParam(String name, T value) {
        fail();
    }

    @Override
    public Map<String, Object> toMap(@NotNull ParamFilter<Object> filter) {
        return fail();
    }

    @Override
    public <T> Map<String, T> toMap(@NotNull Function<Object, T> valueMapper, @NotNull ParamFilter<T> filter) {
        return fail();
    }

    @Override
    public boolean tryManage(@NotNull LivenessReferent referent) {
        return fail();
    }

    @Override
    public boolean tryUnmanage(@NotNull LivenessReferent referent) {
        return fail();
    }

    @Override
    public boolean tryUnmanage(@NotNull Stream<? extends LivenessReferent> referents) {
        return fail();
    }

    @Override
    public boolean tryRetainReference() {
        return fail();
    }

    @Override
    public void dropReference() {
        fail();
    }

    @Override
    public WeakReference<? extends LivenessReferent> getWeakReference() {
        return fail();
    }
}
