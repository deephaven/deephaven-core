package io.deephaven.jsoningester.example;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.ProcessStreamLoggerImpl;
import io.deephaven.jsoningester.JSONToInMemoryTableAdapterBuilder;
import io.deephaven.jsoningester.JSONToTableWriterAdapter;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.type.TypeUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

/**
 * Created by rbasralian on 9/28/22
 */
public class ComplexJsonImportTest2 {


    private static final Logger log =
            ProcessStreamLoggerImpl.makeLogger(() -> DateTimeUtils.currentClock().currentTimeMicros(),
                    TimeZone.getDefault());

    public static void main(String[] args) {

        ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                ComplexJsonImportTest2.class.getName(), log);

        // https://api.weather.gov/stations/KNYC/observations
        // https://api.weather.gov/stations/kcos/observations


        JSONToInMemoryTableAdapterBuilder featuresBuilder = new JSONToInMemoryTableAdapterBuilder();
        // featuresBuilder.processArrays(true)
        featuresBuilder.addColumnFromField("FeatureId", "id", String.class);
        featuresBuilder.addColumnFromField("Type", "type", String.class);

        // builder for 'properties', which has all the actual observations
        JSONToInMemoryTableAdapterBuilder propertiesBuilder = new JSONToInMemoryTableAdapterBuilder();

        propertiesBuilder.addColumnFromField("Station", "station", String.class);
        propertiesBuilder.addColumnFromFunction("Timestamp", Instant.class, value -> Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(value.get("timestamp").textValue())));
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


        final JSONToInMemoryTableAdapterBuilder cloudLayersBuilder = new JSONToInMemoryTableAdapterBuilder();
        cloudLayersBuilder.processArrays(true);

        final JSONToInMemoryTableAdapterBuilder cloudLayersBaseBuilder = new JSONToInMemoryTableAdapterBuilder();
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

        JSONToInMemoryTableAdapterBuilder outerBuilder = new JSONToInMemoryTableAdapterBuilder("observations");
        outerBuilder.addFieldToSubTableMapping("features", featuresBuilder);
        outerBuilder.addColumnFromField("Type", "type", String.class);

        // Create the actual adapter:
        final JSONToInMemoryTableAdapterBuilder.TableAndAdapterBuilderResult result = outerBuilder.build(log);
        final JSONToTableWriterAdapter adapter = result.tableWriterAdapter;

        // Get the output tables:
        final Table observationsTable = result.resultTables.get("observations");
        final Table featuresTable = result.resultTables.get("features");
        final Table cloudsTable = result.resultTables.get("cloudLayers");

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
            } catch (TimeoutException ignored) {
            }
            adapter.cleanup();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Cleaned up");

        final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
        updateGraph.requestRefresh();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        updateGraph.sharedLock().doLocked(() -> {
            System.out.println("observationsTable");
            TableTools.show(observationsTable);
            System.out.println("featuresTable");
            TableTools.show(featuresTable);
            System.out.println("cloudsTable");
            TableTools.show(cloudsTable);
        });

        System.out.println("Exiting");
    }

    public static List<ColumnHeader<?>> addObservationNestedFieldBuilderAndGetColHeaders(
            JSONToInMemoryTableAdapterBuilder parentBuilder,
            String fieldName,
            Class<?> type) {
        // noinspection UnnecessaryLocalVariable
        final String observationName = fieldName;

        JSONToInMemoryTableAdapterBuilder nestedBuilder = new JSONToInMemoryTableAdapterBuilder();
        nestedBuilder.addColumnFromField(observationName, "value", type);
        nestedBuilder.addColumnFromField(observationName + '_' + "unitCode", "unitCode", String.class);
        nestedBuilder.addColumnFromField(observationName + '_' + "qualityControl", "qualityControl", String.class);
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
