/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.custom;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;

public final class CustomApplication2 implements ApplicationState.Factory {

    private final int value;
    @SuppressWarnings("FieldCanBeLocal")
    private LivenessScope scope;

    public CustomApplication2(int value) {
        this.value = value;
    }

    @Override
    public ApplicationState create(Listener appStateListener) {
        final ApplicationState state = new ApplicationState(appStateListener, CustomApplication2.class.getName(),
                CustomApplication2.class.getSimpleName());
        scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            state.setField("app2_value", TableTools.newTable(TableTools.intCol("value", value)));
        }
        return state;
    }
}
