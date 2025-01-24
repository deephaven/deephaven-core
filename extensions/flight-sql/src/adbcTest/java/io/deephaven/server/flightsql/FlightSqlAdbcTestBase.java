//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.deephaven.server.DeephavenServerTestBase;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class FlightSqlAdbcTestBase extends DeephavenServerTestBase {

    private static final Map<String, String> DEEPHAVEN_INT = Map.of(
            "deephaven:isSortable", "true",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "int",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    BufferAllocator allocator;
    AdbcDatabase database;
    AdbcConnection connection;

    @BeforeEach
    void setUp() throws AdbcException {
        final Map<String, Object> options = new HashMap<>();
        AdbcDriver.PARAM_URI.set(options, String.format("grpc://localhost:%d", localPort));
        FlightSqlConnectionProperties.WITH_COOKIE_MIDDLEWARE.set(options, true);
        options.put(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + "Authorization", "Anonymous");
        options.put(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + "x-deephaven-auth-cookie-request", "true");
        allocator = new RootAllocator();
        database = new FlightSqlDriverFactory().getDriver(allocator).open(options);
        connection = database.connect();
    }

    @AfterEach
    void tearDown() throws Exception {
        connection.close();
        database.close();
        allocator.close();
    }

    @Test
    void executeSchema() throws Exception {
        final Schema expectedSchema = new Schema(List
                .of(new Field("Foo", new FieldType(true, Types.MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        try (final AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT 42 as Foo");
            assertThat(statement.executeSchema()).isEqualTo(expectedSchema);
        }
    }

    @Test
    void executeQuery() throws Exception {
        final Schema expectedSchema = new Schema(List
                .of(new Field("Foo", new FieldType(true, Types.MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        try (final AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT 42 as Foo");
            try (final AdbcStatement.QueryResult result = statement.executeQuery()) {
                final ArrowReader reader = result.getReader();
                assertThat(reader.loadNextBatch()).isTrue();
                final VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertThat(root.getSchema()).isEqualTo(expectedSchema);
                final IntVector vector = (IntVector) root.getVector(0);
                assertThat(vector.isNull(0)).isFalse();
                assertThat(vector.get(0)).isEqualTo(42);
                assertThat(reader.loadNextBatch()).isFalse();
            }
        }
    }

    @Test
    void preparedExecuteQuery() throws Exception {
        final Schema expectedSchema = new Schema(List
                .of(new Field("Foo", new FieldType(true, Types.MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        try (final AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT 42 as Foo");
            statement.prepare();
            try (final AdbcStatement.QueryResult result = statement.executeQuery()) {
                final ArrowReader reader = result.getReader();
                assertThat(reader.loadNextBatch()).isTrue();
                final VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertThat(root.getSchema()).isEqualTo(expectedSchema);
                final IntVector vector = (IntVector) root.getVector(0);
                assertThat(vector.isNull(0)).isFalse();
                assertThat(vector.get(0)).isEqualTo(42);
                assertThat(reader.loadNextBatch()).isFalse();
            }
        }
    }
}
