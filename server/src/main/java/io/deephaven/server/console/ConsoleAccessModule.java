package io.deephaven.server.console;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.util.ServiceLoaderUtil;
import io.deephaven.server.session.SessionService;

@Module
public interface ConsoleAccessModule {
    @Provides
    static ConsoleAccess providesAccessControls(SessionService sessionService) {
        return ServiceLoaderUtil.loadOne(sessionService, ConsoleAccess.class)
                .orElseGet(() -> new ConsoleAccessDefaultImpl(sessionService));
    }
}
