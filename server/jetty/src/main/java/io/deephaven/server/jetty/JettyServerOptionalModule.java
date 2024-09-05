//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import dagger.Module;
import io.deephaven.server.flightsql.FlightSqlModule;

@Module(includes = {
        FlightSqlModule.class
})
public interface JettyServerOptionalModule {
}
