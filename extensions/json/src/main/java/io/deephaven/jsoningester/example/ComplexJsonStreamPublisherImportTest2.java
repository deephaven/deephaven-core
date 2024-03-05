package io.deephaven.jsoningester.example;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.ProcessStreamLoggerImpl;
import io.deephaven.jsoningester.JSONToBlinkTableAdapterBuilder;
import io.deephaven.jsoningester.JSONToStreamPublisherAdapter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

public class ComplexJsonStreamPublisherImportTest2 {


    private static final Logger log =
            ProcessStreamLoggerImpl.makeLogger(() -> DateTimeUtils.currentClock().currentTimeMicros(),
                    TimeZone.getDefault());

    public static void main(String[] args) {

        ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                ComplexJsonStreamPublisherImportTest2.class.getName(), log);

        // https://api.weather.gov/stations/KNYC/observations
        // https://api.weather.gov/stations/kcos/observations

        final PeriodicUpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder("MyUpdateGraph").existingOrBuild();

        final ExecutionContext context = ExecutionContext.newBuilder()
                .emptyQueryScope()
                .newQueryLibrary()
                .captureQueryCompiler()
                .setUpdateGraph(updateGraph)
                .build();

        try (SafeCloseable ignored = context.open()) {

            JSONToBlinkTableAdapterBuilder featuresBuilder = new JSONToBlinkTableAdapterBuilder();
            // featuresBuilder.processArrays(true)
            featuresBuilder.addColumnFromField("FeatureId", "id", String.class);
            featuresBuilder.addColumnFromField("Type", "type", String.class);

            // builder for 'properties', which has all the actual observations
            JSONToBlinkTableAdapterBuilder propertiesBuilder = new JSONToBlinkTableAdapterBuilder();

            propertiesBuilder.addColumnFromField("Station", "station", String.class);
            propertiesBuilder.addColumnFromFunction("Timestamp", Instant.class,
                    value -> Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(value.get("timestamp").textValue())));
            // propertiesBuilder.addColumnFromField("timestamp", "Timestamp");
            propertiesBuilder.addColumnFromField("Description", "textDescription", String.class);
            propertiesBuilder.addColumnFromField("RawMessage", "rawMessage", String.class);
            propertiesBuilder.addColumnFromField("PropertiesId", "@id", String.class);

            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "elevation", Integer.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "temperature", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "dewpoint", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windDirection", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windSpeed", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windGust", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder,
                    "barometricPressure", Integer.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "seaLevelPressure", Integer.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "visibility", Integer.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder,
                    "maxTemperatureLast24Hours", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder,
                    "minTemperatureLast24Hours", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder,
                    "precipitationLastHour", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder,
                    "precipitationLast3Hours", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder,
                    "precipitationLast6Hours", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "relativeHumidity", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windChill", Double.class);
            addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "heatIndex", Double.class);


            final JSONToBlinkTableAdapterBuilder cloudLayersBuilder = new JSONToBlinkTableAdapterBuilder();
            cloudLayersBuilder.processArrays(true);

            final JSONToBlinkTableAdapterBuilder cloudLayersBaseBuilder = new JSONToBlinkTableAdapterBuilder();
            cloudLayersBaseBuilder.allowNullValues(true);
            // cloudLayersBaseBuilder.allowMissingKeys(true);
            cloudLayersBaseBuilder.addColumnFromField("base_unitCode", "unitCode", String.class);
            cloudLayersBaseBuilder.addColumnFromField("base", "value", int.class);

            cloudLayersBuilder.addNestedField("base", cloudLayersBaseBuilder);
            cloudLayersBuilder.addColumnFromField("amount", "amount", String.class);

            propertiesBuilder.addFieldToSubTableMapping("cloudLayers", cloudLayersBuilder);

            // The 'properties' (i.e. actual observations) are nested under each element of the 'features' array.
            featuresBuilder.addNestedField("properties", propertiesBuilder);


            // Create DynamicTableWriter for the main table (now that we've defined all its columns)

            JSONToBlinkTableAdapterBuilder outerBuilder = new JSONToBlinkTableAdapterBuilder("observations");
            outerBuilder.addFieldToSubTableMapping("features", featuresBuilder);
            outerBuilder.addColumnFromField("Type", "type", String.class);

            // Create the actual adapter:
            final JSONToBlinkTableAdapterBuilder.TableAndAdapterBuilderResult result = outerBuilder.build(log, true);
            final JSONToStreamPublisherAdapter adapter = result.streamPublisherAdapter;

            final Map<String, Table> resultTables = result.getResultAppendOnlyTables();

            // Get the output tables:
            final Table observationsTable = resultTables.get("observations");
            final Table featuresTable = resultTables.get("features");
            final Table cloudsTable = resultTables.get("cloudLayers");

            TableTools.show(observationsTable);
            TableTools.show(featuresTable);
            TableTools.show(cloudsTable);

            final String json = ExampleJSON.getFullObservationsJson("KNYC");


            System.out.println("Consuming message!");
            adapter.consumeString(json);
            System.out.println("Consumed message!");

            adapter.consumeString(ExampleJSON.getFullObservationsJson("KCOS"));
            System.out.println("Consumed message 2!");


            // try {
            // adapter.consumeMessage("1000",
            // new JSONToTableWriterAdapterBuilder.StringMessageHolder(
            // MicroTimer.currentTimeMicros(),
            // MicroTimer.currentTimeMicros(),
            // ExampleJSON.fullObservationJsonShort));
            // } catch (IOException e) {
            // throw new RuntimeException(e);
            // }
            // System.out.println("Consumed message 3!");

            System.out.println("observationsTable");
            TableTools.show(observationsTable);
            System.out.println("featuresTable");
            TableTools.show(featuresTable);
            System.out.println("cloudsTable");
            TableTools.show(cloudsTable);

            try {
                try {
                    adapter.waitForProcessing(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (TimeoutException ignoredEx) {
                }
                adapter.cleanup();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Cleaned up");

            updateGraph.requestRefresh();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            updateGraph.sharedLock().doLocked(() -> {
                System.out.println("observationsTable");
                TableTools.show(observationsTable, 500);
                System.out.println("featuresTable");
                TableTools.show(featuresTable, 500);
                System.out.println("cloudsTable");
                TableTools.show(cloudsTable, 500);
            });

            System.out.println("Exiting");
        }
    }

    public static void addObservationNestedFieldBuilderAndGetColHeaders(
            JSONToBlinkTableAdapterBuilder parentBuilder,
            String fieldName,
            Class<?> type) {
        // noinspection UnnecessaryLocalVariable
        final String observationName = fieldName;

        JSONToBlinkTableAdapterBuilder nestedBuilder = new JSONToBlinkTableAdapterBuilder();
        nestedBuilder.addColumnFromField(observationName, "value", type);
        nestedBuilder.addColumnFromField(observationName + '_' + "unitCode", "unitCode", String.class);
        nestedBuilder.addColumnFromField(observationName + '_' + "qualityControl", "qualityControl", String.class);
        nestedBuilder.allowMissingKeys(true);
        nestedBuilder.allowNullValues(true);

        parentBuilder.addNestedField(fieldName, nestedBuilder);
    }

}
