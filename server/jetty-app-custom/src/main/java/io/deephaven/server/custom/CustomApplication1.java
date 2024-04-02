//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.custom;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;

import java.util.Objects;

/**
 * Simple application that creates a single-celled string table named {@value #FIELD_NAME}.
 */
public final class CustomApplication1 implements ApplicationState.Factory {
    public static final String FIELD_NAME = "app1_value";

    private final String value;
    @SuppressWarnings("FieldCanBeLocal")
    private LivenessScope scope;

    public CustomApplication1(String value) {
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public ApplicationState create(Listener appStateListener) {
        final ApplicationState state = new ApplicationState(appStateListener, CustomApplication1.class.getName(),
                CustomApplication1.class.getSimpleName());
        scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            state.setField(FIELD_NAME, TableTools.newTable(TableTools.stringCol("value", value)));
        }
        return state;
    }
}
