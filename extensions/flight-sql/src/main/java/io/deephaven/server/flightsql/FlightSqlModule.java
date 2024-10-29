//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.server.session.ActionResolver;
import io.deephaven.server.session.TicketResolver;

/**
 * Binds {@link FlightSqlResolver} as a {@link TicketResolver} and an {@link ActionResolver}.
 */
@Module
public interface FlightSqlModule {

    @Binds
    @IntoSet
    TicketResolver bindFlightSqlAsTicketResolver(FlightSqlResolver resolver);

    @Binds
    @IntoSet
    ActionResolver bindFlightSqlAsActionResolver(FlightSqlResolver resolver);
}
