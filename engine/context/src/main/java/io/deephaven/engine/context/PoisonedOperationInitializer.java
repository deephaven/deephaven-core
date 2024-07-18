//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.util.ExecutionContextRegistrationException;

import java.util.concurrent.Future;

public class PoisonedOperationInitializer implements OperationInitializer {

    public static final PoisonedOperationInitializer INSTANCE = new PoisonedOperationInitializer();

    private <T> T fail() {
        throw ExecutionContextRegistrationException.onFailedComponentAccess("OperationInitializer");
    }

    @Override
    public boolean canParallelize() {
        return fail();
    }

    @Override
    public Future<?> submit(Runnable runnable) {
        return fail();
    }

    @Override
    public int parallelismFactor() {
        return fail();
    }
}
