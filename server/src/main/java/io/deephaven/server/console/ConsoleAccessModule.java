package io.deephaven.server.console;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.access.Helper;
import io.deephaven.server.session.SessionService;

@Module
public interface ConsoleAccessModule {
    @Provides
    static ConsoleAccess providesAccessControls(SessionService sessionService) {
        return Helper.findOne(sessionService, ConsoleAccess.class)
                .orElseGet(() -> new ConsoleAccessDefaultImpl(sessionService));
    }
}
