package io.deephaven.server.table;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.access.Helper;
import io.deephaven.server.session.SessionService;

@Module
public interface TableAccessModule {

    @Provides
    static TableAccess providesAccessControls(SessionService sessionService) {
        return Helper.findOne(sessionService, TableAccess.class)
                .orElseGet(() -> new TableAccessDefaultImpl(sessionService));
    }
}
