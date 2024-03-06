//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.authentication.sql;

import io.deephaven.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Contains the configurable assumptions of this project - the JDBC connection string, properties to pass to that
 * connection, basic structure of the tables to use.
 */
public class BasicSqlConfig {
    public static final String JDBC_CONNECTION_STRING =
            Configuration.getInstance().getProperty("authentication.basic.sql.jdbc.connection");
    public static final Properties JDBC_CONNECTION_PROPS =
            Configuration.getInstance().getProperties("authentication.basic.sql.jdbc.");
    public static final String SCHEMA = "deephaven_username_password_auth";
    public static final String USER_TABLE = SCHEMA + ".users";
    public static final String USER_NAME_COLUMN = "username";
    public static final String USER_PASSWORD_COLUMN = "password_hash";
    public static final String USER_ROUNDS_COLUMN = "rounds";

    public static final int BCRYPT_LOG_ROUNDS =
            Configuration.getInstance().getIntegerWithDefault("authentication.basic.bcrypt.rounds", 10);

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_CONNECTION_STRING, JDBC_CONNECTION_PROPS);
    }
}
