//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.deephaven.server.DeephavenServerTestBase;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public abstract class FlightSqlJdbcTestBase extends DeephavenServerTestBase {

    private String jdbcUrl(boolean requestCookie) {
        return String.format(
                "jdbc:arrow-flight-sql://localhost:%d/?Authorization=Anonymous&useEncryption=false%s",
                localPort,
                (requestCookie ? "&x-deephaven-auth-cookie-request=true" : ""));
    }

    private Connection connect(boolean requestCookie) throws SQLException {
        return DriverManager.getConnection(jdbcUrl(requestCookie));
    }

    @Test
    void execute() throws SQLException {
        try (
                final Connection connection = connect(true);
                final Statement statement = connection.createStatement()) {
            if (statement.execute("SELECT 1 as Foo, 2 as Bar")) {
                consume(statement.getResultSet(), 2, 1);
            }
        }
    }

    @Test
    void executeQuery() throws SQLException {
        try (
                final Connection connection = connect(true);
                final Statement statement = connection.createStatement()) {
            consume(statement.executeQuery("SELECT 1 as Foo, 2 as Bar"), 2, 1);
        }
    }

    @Test
    void executeUpdate() throws SQLException {
        try (
                final Connection connection = connect(true);
                final Statement statement = connection.createStatement()) {
            try {
                statement.executeUpdate("INSERT INTO fake(name) VALUES('Smith')");
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                assertThat((Throwable) e).getRootCause()
                        .hasMessageContaining("Object 'fake' not found");
            }
        }
    }

    @Test
    void preparedExecute() throws SQLException {
        try (
                final Connection connection = connect(true);
                final PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1 as Foo, 2 as Bar")) {
            if (preparedStatement.execute()) {
                consume(preparedStatement.getResultSet(), 2, 1);
            }
            consume(preparedStatement.executeQuery(), 2, 1);
            try {
                preparedStatement.executeUpdate();
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                assertThat((Throwable) e).getRootCause()
                        .hasMessageContaining("Flight SQL descriptors cannot be published to");
            }
        }
    }

    @Test
    void preparedExecuteQuery() throws SQLException {
        try (
                final Connection connection = connect(true);
                final PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1 as Foo, 2 as Bar")) {
            consume(preparedStatement.executeQuery(), 2, 1);
        }
    }

    @Test
    void preparedUpdate() throws SQLException {
        try (
                final Connection connection = connect(true);
                final PreparedStatement preparedStatement =
                        connection.prepareStatement("INSERT INTO fake(name) VALUES('Smith')")) {
            try {
                preparedStatement.executeUpdate();
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                assertThat((Throwable) e).getRootCause()
                        .hasMessageContaining("Flight SQL descriptors cannot be published to");
            }
        }
    }

    @Test
    void executeQueryNoCookie() throws SQLException {
        try (final Connection connection = connect(false)) {
            final Statement statement = connection.createStatement();
            try {
                statement.executeQuery("SELECT 1 as Foo, 2 as Bar");
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                assertThat((Throwable) e).getRootCause()
                        .hasMessageContaining(
                                "Flight SQL: Must use the original session; is the client echoing the authentication token properly?");
            }
            try {
                statement.close();
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                assertThat((Throwable) e).getRootCause()
                        .hasMessageContaining(
                                "Flight SQL: Must use the original session; is the client echoing the authentication token properly?");
            }
        }
    }

    @Test
    void preparedExecuteQueryNoCookie() throws SQLException {
        try (final Connection connection = connect(false)) {
            final PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1 as Foo, 2 as Bar");
            try {
                preparedStatement.executeQuery();
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (RuntimeException e) {
                assertHelpfulClientErrorMessage(e);
            }
            // If our authentication is bad, we won't be able to close the prepared statement either. If we want to
            // solve for this scenario, we would probably need to use randomized handles for the prepared statements
            // (instead of incrementing handle ids).
            try {
                preparedStatement.close();
                failBecauseExceptionWasNotThrown(RuntimeException.class);
            } catch (RuntimeException e) {
                assertHelpfulClientErrorMessage(e);
            }
        }
    }

    private static void assertHelpfulClientErrorMessage(RuntimeException e) {
        // Note: this is arguably a JDBC implementation bug; it should be throwing java.sql.SQLException, but it's
        // exposing shadowed internal error from Flight.
        assertThat(e.getClass().getName()).isEqualTo(
                "org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightRuntimeException");
        assertThat(e).hasMessageContaining(
                "Flight SQL: Must use the original session; is the client echoing the authentication token properly?");
    }

    private static void consume(ResultSet rs, int numCols, int numRows) throws SQLException {
        final ResultSetMetaData rsmd = rs.getMetaData();
        assertThat(rsmd.getColumnCount()).isEqualTo(numCols);
        int rows = 0;
        while (rs.next()) {
            ++rows;
        }
        assertThat(rows).isEqualTo(numRows);
    }
}
