//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner.scheduler;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.util.Scheduler;

@Module(includes = {SchedulerModule.class})
public interface SchedulerDelegatingImplModule {

    @Provides
    static Scheduler.DelegatingImpl castsToDelegatingImpl(Scheduler scheduler) {
        return (Scheduler.DelegatingImpl) scheduler;
    }
}
