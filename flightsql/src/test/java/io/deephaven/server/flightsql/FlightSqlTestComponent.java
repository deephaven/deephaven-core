//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.plugin.Registration;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionServiceGrpcImpl;
import io.deephaven.server.test.TestAuthModule;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.grpc.ServerInterceptor;

import java.util.Set;

public interface FlightSqlTestComponent {
    Set<ServerInterceptor> interceptors();

    SessionServiceGrpcImpl sessionGrpcService();

    SessionService sessionService();

    GrpcServer server();

    TestAuthModule.BasicAuthTestImpl basicAuthHandler();

    ExecutionContext executionContext();

    TestAuthorizationProvider authorizationProvider();

    Registration.Callback registration();
}
