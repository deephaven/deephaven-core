//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.deephaven.server.DeephavenServerTestBase;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public abstract class FlightSqlJdbcUnauthenticatedTestBase extends DeephavenServerTestBase {
    private String jdbcUrl() {
        return String.format(
                "jdbc:arrow-flight-sql://localhost:%d/?useEncryption=false",
                localPort);
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl());
    }

    @Test
    void executeQuery() throws SQLException {
        // uses prepared statement internally
        try (
                final Connection connection = connect();
                final Statement statement = connection.createStatement()) {
            try {
                statement.executeQuery("SELECT 1 as Foo, 2 as Bar");
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                unauthenticated(e);
            }
        }
    }

    @Test
    void execute() throws SQLException {
        // uses prepared statement internally
        try (
                final Connection connection = connect();
                final Statement statement = connection.createStatement()) {
            try {
                statement.execute("SELECT 1 as Foo, 2 as Bar");
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                unauthenticated(e);
            }
        }
    }

    @Test
    void executeUpdate() throws SQLException {
        // uses prepared statement internally
        try (
                final Connection connection = connect();
                final Statement statement = connection.createStatement()) {
            try {
                statement.executeUpdate("INSERT INTO fake(name) VALUES('Smith')");
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                unauthenticated(e);
            }
        }
    }

    @Test
    void prepareStatement() throws SQLException {
        try (
                final Connection connection = connect()) {
            try {
                connection.prepareStatement("SELECT 1");
            } catch (SQLException e) {
                unauthenticated(e);
            }
        }
    }

    private static void unauthenticated(SQLException e) {
        assertThat((Throwable) e).getRootCause()
                .hasMessageContaining("Flight SQL: Must be authenticated");
    }
}
