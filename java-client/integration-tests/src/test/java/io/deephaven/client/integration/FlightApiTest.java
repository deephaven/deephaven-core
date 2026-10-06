//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.BlinkInputTable;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The flight layer against a real server: DoGet, schemas, listing, DoPut, and the three kinds of input table.
 */
class FlightApiTest {

    private static final TableHeader KEY_VALUE = TableHeader.of(ColumnHeader.ofString("K"), ColumnHeader.ofInt("V"));

    private static BufferAllocator allocator;
    private static ScheduledExecutorService scheduler;
    private static FlightSessionFactoryConfig.Factory factory;

    @BeforeAll
    static void connect() {
        allocator = new RootAllocator();
        scheduler = Executors.newScheduledThreadPool(4);
        factory = FlightSessionFactoryConfig.builder()
                .clientConfig(TestServer.clientConfig())
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
    }

    @AfterAll
    static void disconnect() {
        factory.managedChannel().shutdownNow();
        scheduler.shutdownNow();
        // Throws if any Arrow buffer the tests read was not released
        allocator.close();
    }

    @Test
    void doGetStreamsEveryRow() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(TableSpec.empty(5).view("I=ii"))) {
            assertThat(column(flight, handle, "I")).containsExactly(0L, 1L, 2L, 3L, 4L);
        }
    }

    @Test
    void schemaByPathNamesTheColumns() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(TableSpec.empty(1).view("I=ii", "S=`x`"))) {
            flight.session().publish("api_flight_schema", handle).get(10, TimeUnit.SECONDS);
            assertThat(flight.schema(new ScopeId("api_flight_schema")).getFields())
                    .extracting(Field::getName)
                    .containsExactly("I", "S");
        }
    }

    @Test
    void listIncludesPublishedTables() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(TableSpec.empty(1))) {
            flight.session().publish("api_flight_listed", handle).get(10, TimeUnit.SECONDS);
            final List<List<String>> paths = new ArrayList<>();
            for (FlightInfo info : flight.list()) {
                paths.add(info.getDescriptor().getPath());
            }
            assertThat(paths).contains(List.of("scope", "api_flight_listed"));
        }
    }

    @Test
    void doPutRoundTripsClientData() throws Exception {
        final NewTable table = ColumnHeader.ofInt("X").start(3).row(10).row(20).row(30).newTable();
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.putExport(table, allocator)) {
            assertThat(handle.response().getSize()).isEqualTo(3);
            assertThat(column(flight, handle, "X")).containsExactly(10, 20, 30);
        }
    }

    @Test
    void appendOnlyInputTableKeepsEveryRow() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle input = flight.session().execute(InMemoryAppendOnlyInputTable.of(KEY_VALUE))) {
            flight.addToInputTable(input, rows("a", 1), allocator).get(10, TimeUnit.SECONDS);
            flight.addToInputTable(input, rows("a", 2), allocator).get(10, TimeUnit.SECONDS);
            awaitColumn(flight, input, "V", List.of(1, 2));
        }
    }

    @Test
    void keyBackedInputTableUpsertsAndDeletesByKey() throws Exception {
        final TableSpec spec = InMemoryKeyBackedInputTable.of(KEY_VALUE, Collections.singletonList("K"));
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle input = flight.session().execute(spec)) {
            flight.addToInputTable(input, rows("a", 1), allocator).get(10, TimeUnit.SECONDS);
            flight.addToInputTable(input, rows("a", 2), allocator).get(10, TimeUnit.SECONDS);
            awaitColumn(flight, input, "V", List.of(2));

            final NewTable deleteA = ColumnHeader.ofString("K").start(1).row("a").newTable();
            flight.deleteFromInputTable(input, deleteA, allocator).get(10, TimeUnit.SECONDS);
            awaitColumn(flight, input, "V", List.of());
        }
    }

    @Test
    void blinkInputTableRowsShowUpInATail() throws Exception {
        final BlinkInputTable blink = BlinkInputTable.of(KEY_VALUE);
        try (final FlightSession flight = factory.newFlightSession()) {
            final List<TableHandle> handles = flight.session().execute(List.of(blink, blink.tail(10)));
            try (
                    final TableHandle input = handles.get(0);
                    final TableHandle tail = handles.get(1)) {
                flight.addToInputTable(input, rows("a", 1), allocator).get(10, TimeUnit.SECONDS);
                flight.addToInputTable(input, rows("b", 2), allocator).get(10, TimeUnit.SECONDS);
                // The blink table itself empties every cycle; the tail keeps what passed through
                awaitColumn(flight, tail, "V", List.of(1, 2));
            }
        }
    }

    private static NewTable rows(String key, int value) {
        return ColumnHeader.ofString("K").header(ColumnHeader.ofInt("V")).row(key, value).newTable();
    }

    /** DoGet the table and return one column's values, in row order. */
    private static List<Object> column(FlightSession flight, HasTicketId ticket, String column) throws Exception {
        final List<Object> values = new ArrayList<>();
        try (final FlightStream stream = flight.stream(ticket)) {
            while (stream.next()) {
                final VectorSchemaRoot root = stream.getRoot();
                final FieldVector vector = root.getVector(column);
                for (int i = 0; i < root.getRowCount(); ++i) {
                    values.add(vector.getObject(i));
                }
            }
        }
        return values;
    }

    /**
     * Input table changes land on the next update cycle, so poll DoGet until the column matches or time runs out.
     */
    private static void awaitColumn(FlightSession flight, HasTicketId ticket, String column, List<?> expected)
            throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        List<Object> actual = column(flight, ticket, column);
        while (!actual.equals(expected) && System.nanoTime() < deadline) {
            Thread.sleep(100);
            actual = column(flight, ticket, column);
        }
        assertThat(actual).isEqualTo(expected);
    }
}
