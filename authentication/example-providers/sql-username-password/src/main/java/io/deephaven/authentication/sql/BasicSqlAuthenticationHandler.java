//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.authentication.sql;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.BasicAuthMarshaller;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.mindrot.jbcrypt.BCrypt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * If the user isn't found or the password doesn't match, reject with an exception. Note that the calling code may
 * decide to treat this rejection as "might be the wrong handler" instead, as usage of basic auth can technically be
 * confused with other forms of handshake messages.
 */
public class BasicSqlAuthenticationHandler implements BasicAuthMarshaller.Handler {
    private static final Logger logger = LoggerFactory.getLogger(BasicSqlAuthenticationHandler.class);

    private static final String SELECT_USER =
            "select " + BasicSqlConfig.USER_PASSWORD_COLUMN + "," + BasicSqlConfig.USER_ROUNDS_COLUMN + " from "
                    + BasicSqlConfig.USER_TABLE + " where " + BasicSqlConfig.USER_NAME_COLUMN + " = ?";

    @Override
    public Optional<AuthContext> login(String username, String password) throws AuthenticationException {
        try (Connection connection = BasicSqlConfig.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement =
                    connection.prepareStatement(SELECT_USER, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
                statement.setString(1, username);
                ResultSet resultSet = statement.executeQuery();
                if (!resultSet.next()) {
                    // No user found, might not have meant to be a query to find this user
                    return Optional.empty();
                }

                String foundHash = resultSet.getString(1);
                int foundRounds = resultSet.getInt(2);
                if (resultSet.next()) {
                    logger.error().append("Duplicate rows found for user! ").append(username).endl();
                    return Optional.empty();
                }
                // compute the replacement hash before comparing, to make failures take about as long as successes
                String replacementHash = BCrypt.hashpw(password, BCrypt.gensalt(BasicSqlConfig.BCRYPT_LOG_ROUNDS));

                // actually check if the given password fits
                if (!BCrypt.checkpw(password, foundHash)) {
                    // User found, password does not match, explicitly throw an exception
                    throw new AuthenticationException();
                }

                // if the user had the correct pw, strengthen the stored hash if necessary
                if (foundRounds < BasicSqlConfig.BCRYPT_LOG_ROUNDS) {
                    resultSet.updateString(0, replacementHash);
                }

                return Optional.of(new AuthContext.SuperUser());// TODO actual user object, with a name and roles and
                                                                // stuff
            }

        } catch (SQLException ex) {
            // Log and return empty, this likely is a configuration issue rather than the user's fault
            logger.error().append("Error querying login details").append(ex).endl();
            return Optional.empty();
        }
    }

    public static void main(String[] args) {
        for (String arg : args) {
            System.out.println(BCrypt.hashpw(arg, BCrypt.gensalt(BasicSqlConfig.BCRYPT_LOG_ROUNDS)));
        }
    }
}
