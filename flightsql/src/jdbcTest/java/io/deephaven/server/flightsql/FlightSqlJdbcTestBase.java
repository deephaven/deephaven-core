//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import org.junit.jupiter.api.Disabled;
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

public abstract class FlightSqlJdbcTestBase extends FlightSqlTestBase {

    private String jdbcUrl() {
        return String.format(
                "jdbc:arrow-flight-sql://localhost:%d/?Authorization=Anonymous&useEncryption=false&x-deephaven-auth-cookie-request=true",
                localPort);
    }

    private Connection jdbcConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl());
    }

    @Disabled("Need to update Arrow FlightSQL JDBC version - this one tries to execute this as an UPDATE (doPut)")
    @Test
    void executeSelect1() throws SQLException {
        try (
                final Connection connection = jdbcConnection();
                final Statement statement = connection.createStatement()) {
            if (statement.execute("SELECT 1")) {
                printResultSet(statement.getResultSet());
            }
        }
    }

    // this one is even dumber than above; we are saying executeQuery _not_ executeUpdate... :/
    @Disabled("Need to update Arrow FlightSQL JDBC version - this one tries to execute this as an UPDATE (doPut)")
    @Test
    void executeQuerySelect1() throws SQLException {
        try (
                final Connection connection = jdbcConnection();
                final Statement statement = connection.createStatement()) {
            printResultSet(statement.executeQuery("SELECT 1"));
        }
    }

    @Test
    void executeUpdate() throws SQLException {
        try (
                final Connection connection = jdbcConnection();
                final Statement statement = connection.createStatement()) {
            try {
                statement.executeUpdate("INSERT INTO fake(name) VALUES('Smith')");
                failBecauseExceptionWasNotThrown(SQLException.class);
            } catch (SQLException e) {
                assertThat((Throwable) e).getRootCause()
                        .hasMessageContaining("FlightSQL descriptors cannot be published to");
            }
        }
    }

    @Test
    void select1Prepared() throws SQLException {
        try (
                final Connection connection = jdbcConnection();
                final PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1")) {
            if (preparedStatement.execute()) {
                printResultSet(preparedStatement.getResultSet());
            }
        }
    }

    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }
}
