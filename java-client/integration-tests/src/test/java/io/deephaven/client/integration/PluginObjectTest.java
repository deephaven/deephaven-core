//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.TableObject;
import io.deephaven.client.impl.TypedTicket;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The object service against a plugin object the server image supports: a Figure. Fetching it by typed ticket returns
 * its descriptor as bytes and the table it plots as an export, which can then be read over Flight. This is the API
 * under the {@code fetch-object} and {@code convert-to-table} examples.
 */
class PluginObjectTest {

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
        allocator.close();
    }

    @Test
    void fetchedFigureCarriesItsDescriptorAndExportsItsTable() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final ConsoleSession console = flight.session().console("python").get(10, TimeUnit.SECONDS)) {
            console.executeCode(String.join("\n",
                    "from deephaven import empty_table",
                    "from deephaven.plot.figure import Figure",
                    "api_figure_table = empty_table(5).update('I = ii')",
                    "api_figure = Figure().plot_xy(series_name='api', t=api_figure_table, x='I', y='I').show()"));

            final TypedTicket figure = new TypedTicket("Figure", new ScopeId("api_figure"));
            try (final ServerData fetched = flight.session().fetch(figure).get(10, TimeUnit.SECONDS)) {
                // The payload is the figure's descriptor, a protobuf
                final byte[] bytes = new byte[fetched.data().remaining()];
                fetched.data().slice().get(bytes);
                final FigureDescriptor descriptor = FigureDescriptor.parseFrom(bytes);
                assertThat(descriptor.getChartsCount()).isEqualTo(1);
                assertThat(descriptor.getCharts(0).getSeriesList())
                        .extracting(FigureDescriptor.SeriesDescriptor::getName)
                        .containsExactly("api");

                // The one export is the plotted table, readable like any other
                assertThat(fetched.exports()).hasSize(1);
                assertThat(fetched.exports().get(0)).isInstanceOf(TableObject.class);
                final TableObject table = (TableObject) fetched.exports().get(0);
                final List<Long> values = new ArrayList<>();
                try (final FlightStream stream = flight.stream(table)) {
                    while (stream.next()) {
                        final BigIntVector i = (BigIntVector) stream.getRoot().getVector("I");
                        for (int r = 0; r < stream.getRoot().getRowCount(); ++r) {
                            values.add(i.get(r));
                        }
                    }
                }
                assertThat(values).containsExactly(0L, 1L, 2L, 3L, 4L);
            }
        }
    }

    @Test
    void fetchingWithTheWrongTypeFails() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final ConsoleSession console = flight.session().console("python").get(10, TimeUnit.SECONDS)) {
            console.executeCode("from deephaven import empty_table\napi_not_a_figure = empty_table(1)");
            final TypedTicket wrongType = new TypedTicket("Figure", new ScopeId("api_not_a_figure"));
            // The server reports a table that is not a Figure as the Figure type not being found for it
            assertThatThrownBy(() -> flight.session().fetch(wrongType).get(10, TimeUnit.SECONDS).close())
                    .hasStackTraceContaining("NOT_FOUND")
                    .hasStackTraceContaining("expected type 'Figure'");
        }
    }
}
