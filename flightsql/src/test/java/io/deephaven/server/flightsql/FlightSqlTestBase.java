//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.server.runner.MainHelper;
import io.deephaven.util.SafeCloseable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public abstract class FlightSqlTestBase {

    protected FlightSqlTestComponent component;

    private LogBuffer logBuffer;
    private SafeCloseable executionContext;
    private GrpcServer server;
    protected int localPort;

    protected abstract FlightSqlTestComponent component();

    @BeforeAll
    public static void setupOnce() throws IOException {
        MainHelper.bootstrapProjectDirectories();
    }

    @BeforeEach
    public void setup() throws IOException {
        logBuffer = new LogBuffer(128);
        LogBufferGlobal.setInstance(logBuffer);
        component = component();
        executionContext = component.executionContext().open();
        server = component.server();
        server.start();
        localPort = server.getPort();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        server.stopWithTimeout(1, TimeUnit.MINUTES);
        server.join();
        executionContext.close();
        LogBufferGlobal.clear(logBuffer);
    }
}
