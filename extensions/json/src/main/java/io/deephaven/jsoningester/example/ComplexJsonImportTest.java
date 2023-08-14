package io.deephaven.jsoningester.example;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
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
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

/**
 * Created by rbasralian on 9/28/22
 */
public class ComplexJsonImportTest {


    private static final Logger log =
            ProcessStreamLoggerImpl.makeLogger(() -> Clock.system().currentTimeMicros(), TimeZone.getDefault());

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {

        ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                ComplexJsonImportTest.class.getName(),
                log);

        // https://api.weather.gov/stations/KNYC/observations
        // https://api.weather.gov/stations/kcos/observations


        JSONToInMemoryTableAdapterBuilder mainBuilder = new JSONToInMemoryTableAdapterBuilder();
        mainBuilder.addColumnFromField("Id", "id", String.class);
        mainBuilder.addColumnFromField("Type", "type", String.class);

        // builder for 'properties', which has all the actual observations
        JSONToInMemoryTableAdapterBuilder propertiesBuilder = new JSONToInMemoryTableAdapterBuilder();

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
        addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "maxTemperatureLast24Hours", Double.class);
        addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "minTemperatureLast24Hours", Double.class);
        addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLastHour", Double.class);
        addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLast3Hours", Double.class);
        addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLast6Hours", Double.class);
        addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "relativeHumidity", Double.class);
        addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windChill", Double.class);
        addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "heatIndex", Double.class);


        final JSONToInMemoryTableAdapterBuilder cloudLayersBuilder = new JSONToInMemoryTableAdapterBuilder();
        cloudLayersBuilder.processArrays(true);

        final JSONToInMemoryTableAdapterBuilder cloudLayersBaseBuilder = new JSONToInMemoryTableAdapterBuilder();
        cloudLayersBaseBuilder.addColumnFromField("base_unitCode", "unitCode", String.class);
        cloudLayersBaseBuilder.addColumnFromField("base", "value", int.class);

        cloudLayersBuilder.addNestedField("base", cloudLayersBaseBuilder);
        cloudLayersBuilder.addColumnFromField("amount", "amount", String.class);

        propertiesBuilder.addFieldToSubTableMapping("cloudLayers", cloudLayersBuilder);

        mainBuilder.addNestedField("properties", propertiesBuilder);

        final JSONToInMemoryTableAdapterBuilder.TableAndAdapterBuilderResult adapterAndTables = mainBuilder.build(log);
        final JSONToTableWriterAdapter adapter = adapterAndTables.tableWriterAdapter;
        final Map<String, Table> resultTables = adapterAndTables.resultTables;

        // Get the output tables:
        final Table mainTable = Require.neqNull(resultTables.get("MAIN_TABLE"), "mainTable");
        final Table cloudLayers = Require.neqNull(resultTables.get("cloudLayers"), "cloudLayers");

        TableTools.show(mainTable);
        TableTools.show(cloudLayers);

        System.out.println("Consuming message!");
        adapter.consumeString(ExampleJSON.observationsJson);
        System.out.println("Consumed message!");

        TableTools.show(mainTable);
        TableTools.show(cloudLayers);

        adapter.waitForProcessing(100000);
        adapter.cleanup();

        System.out.println("Cleaned up");

        final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
        updateGraph.requestRefresh();
        updateGraph.sharedLock().doLocked(() -> {
            TableTools.show(mainTable);
            TableTools.show(cloudLayers);
        });

        System.out.println("Exiting");
    }

    public static List<ColumnHeader<?>> addObservationNestedFieldBuilderAndGetColHeaders(
            JSONToInMemoryTableAdapterBuilder parentBuilder,
            String fieldName,
            Class<?> type) {
        final String observationName = fieldName;

        JSONToInMemoryTableAdapterBuilder nestedBuilder = new JSONToInMemoryTableAdapterBuilder();
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
