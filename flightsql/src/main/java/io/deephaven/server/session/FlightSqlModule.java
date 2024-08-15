//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;

@Module
public interface FlightSqlModule {

    @Binds
    @IntoSet
    TicketResolver bindFlightSqlTicketResolver(FlightSqlTicketResolver resolver);
}
