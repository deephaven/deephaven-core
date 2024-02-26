package io.deephaven.jsoningester.example;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.ProcessStreamLoggerImpl;
import io.deephaven.jsoningester.JSONToBlinkTableAdapterBuilder;
import io.deephaven.jsoningester.JSONToStreamPublisherAdapter;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.type.TypeUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

public class ComplexJsonStreamPublisherImportTest {


    private static final Logger log =
            ProcessStreamLoggerImpl.makeLogger(() -> Clock.system().currentTimeMicros(), TimeZone.getDefault());

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {

        ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                ComplexJsonStreamPublisherImportTest.class.getName(),
                log);

        // https://api.weather.gov/stations/KNYC/observations
        // https://api.weather.gov/stations/kcos/observations

        // final ExecutionContext executionContext = ExecutionContext.getDefaultContext();

        final PeriodicUpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder("MyUpdateGraph").existingOrBuild();

        final ExecutionContext context = ExecutionContext.newBuilder()
                .emptyQueryScope()
                .newQueryLibrary()
                .captureQueryCompiler()
                .setUpdateGraph(updateGraph)
                .build();

        try (SafeCloseable ignored = context.open()) {

            JSONToBlinkTableAdapterBuilder mainBuilder = new JSONToBlinkTableAdapterBuilder("observations");
            mainBuilder.addColumnFromField("Id", "id", String.class);
            mainBuilder.addColumnFromField("Type", "type", String.class);

            // builder for 'properties', which has all the actual observations
            JSONToBlinkTableAdapterBuilder propertiesBuilder = new JSONToBlinkTableAdapterBuilder();

            propertiesBuilder.addColumnFromField("Station", "station", String.class);
            propertiesBuilder.addColumnFromFunction("Timestamp", Instant.class,
                    value -> Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(value.get("timestamp").textValue())));
            // propertiesBuilder.addColumnFromField("timestamp", "Timestamp");
            propertiesBuilder.addColumnFromField("Description", "textDescription", String.class);
            propertiesBuilder.addColumnFromField("RawMessage", "rawMessage", String.class);

            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "elevation", Integer.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "temperature", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "dewpoint", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windDirection", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windSpeed", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windGust", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "barometricPressure", Integer.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "seaLevelPressure", Integer.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "visibility", Integer.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "maxTemperatureLast24Hours",
                    Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "minTemperatureLast24Hours",
                    Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLastHour", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLast3Hours",
                    Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLast6Hours",
                    Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "relativeHumidity", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windChill", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "heatIndex", Double.class);


            final JSONToBlinkTableAdapterBuilder cloudLayersBuilder = new JSONToBlinkTableAdapterBuilder();
            cloudLayersBuilder.processArrays(true);

            final JSONToBlinkTableAdapterBuilder cloudLayersBaseBuilder = new JSONToBlinkTableAdapterBuilder();
            cloudLayersBaseBuilder.addColumnFromField("base_unitCode", "unitCode", String.class);
            cloudLayersBaseBuilder.addColumnFromField("base", "value", int.class);

            cloudLayersBuilder.addNestedField("base", cloudLayersBaseBuilder);
            cloudLayersBuilder.addColumnFromField("amount", "amount", String.class);

            propertiesBuilder.addFieldToSubTableMapping("cloudLayers", cloudLayersBuilder);

            mainBuilder.addNestedField("properties", propertiesBuilder);

            final JSONToBlinkTableAdapterBuilder.TableAndAdapterBuilderResult adapterAndTables =
                    mainBuilder.build(log, true);
            final JSONToStreamPublisherAdapter adapter = adapterAndTables.streamPublisherAdapter;
            final Map<String, Table> resultTables = adapterAndTables.getResultAppendOnlyTables();

            // Get the output tables:
            final Table observations = Require.neqNull(resultTables.get("observations"), "observations");
            final Table cloudLayers = Require.neqNull(resultTables.get("cloudLayers"), "cloudLayers");

            TableTools.show(observations);
            TableTools.show(cloudLayers);

            System.out.println("Consuming message!");
            adapter.consumeString(ExampleJSON.observationsJson);
            System.out.println("Consumed message!");

            System.out.println("Printing tables. (Data may not be processed yet.)");
            TableTools.show(observations);
            TableTools.show(cloudLayers);

            adapter.waitForProcessing(100000);
            adapter.cleanup();

            System.out.println("Cleaned up");

            updateGraph.requestRefresh();

            System.out.println("UpdateGraph refreshed. Printing tables. (They should have data now.)");
            updateGraph.sharedLock().doLocked(() -> {
                TableTools.show(observations);
                TableTools.show(cloudLayers);
            });

            System.out.println("Calling shutdown()");
            adapterAndTables.shutdown();
            System.out.println("Exiting");

        }
    }

    public static List<ColumnHeader<?>> addObservationNestedFieldBuilderAndGetColHeaders(
            JSONToBlinkTableAdapterBuilder parentBuilder,
            String fieldName,
            Class<?> type) {
        final String observationName = fieldName;

        JSONToBlinkTableAdapterBuilder nestedBuilder = new JSONToBlinkTableAdapterBuilder();
        nestedBuilder.addColumnFromField(observationName, "value", type);
        nestedBuilder.addColumnFromField(observationName + '_' + "unitCode", "unitCode", type);
        nestedBuilder.addColumnFromField(observationName + '_' + "qualityControl", "qualityControl", type);
        nestedBuilder.allowMissingKeys(true);
        nestedBuilder.allowNullValues(true);

        ColumnHeader<?> firstColHeader;

        type = TypeUtils.getBoxedType(type);
        if (String.class.equals(type)) {
            firstColHeader = ColumnHeader.ofString(observationName);
        } else if (Double.class.equals(type)) {
            firstColHeader = ColumnHeader.ofDouble(observationName);
        } else if (Integer.class.equals(type)) {
            firstColHeader = ColumnHeader.ofInt(observationName);
        } else if (Long.class.equals(type)) {
            firstColHeader = ColumnHeader.ofLong(observationName);
        } else if (Boolean.class.equals(type)) {
            firstColHeader = ColumnHeader.ofBoolean(observationName);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type.getName());
        }

        parentBuilder.addNestedField(fieldName, nestedBuilder);

        return List.of(
                firstColHeader,
                ColumnHeader.ofString(observationName + "_unitCode"),
                ColumnHeader.ofString(observationName + "_qualityControl"));
    }

}
