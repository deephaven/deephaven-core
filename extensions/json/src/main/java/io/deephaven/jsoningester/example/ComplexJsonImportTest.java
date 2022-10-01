package io.deephaven.jsoningester.example;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.UpdateSourceQueryTable;
import io.deephaven.engine.table.impl.util.DynamicTableWriter;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.ProcessStreamLoggerImpl;
import io.deephaven.jsoningester.JSONToTableWriterAdapter;
import io.deephaven.jsoningester.JSONToTableWriterAdapterBuilder;
import io.deephaven.jsoningester.MessageToTableWriterAdapter;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.clock.MicroTimer;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.type.TypeUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Created by rbasralian on 9/28/22
 */
public class ComplexJsonImportTest {


    private static final Logger log = ProcessStreamLoggerImpl.makeLogger(MicroTimer::currentTimeMicros, TimeZone.getDefault());

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {

        ProcessEnvironment.basicServerInitialization(Configuration.getInstance(), ComplexJsonImportTest.class.getName(), log);

        // https://api.weather.gov/stations/KNYC/observations
        // https://api.weather.gov/stations/kcos/observations


        JSONToTableWriterAdapterBuilder mainBuilder = new JSONToTableWriterAdapterBuilder();
        mainBuilder.autoValueMapping(false);
        mainBuilder.addColumnFromField("Id", "id");
        mainBuilder.addColumnFromField("Type", "type");

        List<ColumnHeader<?>> mainTableColHeaders = new ArrayList<>(32);

        mainTableColHeaders.add(ColumnHeader.ofString("Id"));
        mainTableColHeaders.add(ColumnHeader.ofString("Type"));
        mainTableColHeaders.add(ColumnHeader.of("Timestamp", DateTime.class));

        // builder for 'properties', which has all the actual observations
        JSONToTableWriterAdapterBuilder propertiesBuilder = new JSONToTableWriterAdapterBuilder();

        mainTableColHeaders.add(ColumnHeader.ofString("Station"));
        mainTableColHeaders.add(ColumnHeader.ofString("Description"));
        mainTableColHeaders.add(ColumnHeader.ofString("RawMessage"));

        propertiesBuilder.autoValueMapping(false);
        propertiesBuilder.addColumnFromField("Station", "station");
        propertiesBuilder.addColumnFromFunction("Timestamp", DateTime.class, value -> DateTimeUtils.instantToTime(Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(value.get("timestamp").textValue()))));
//        propertiesBuilder.addColumnFromField("timestamp", "Timestamp");
        propertiesBuilder.addColumnFromField("Description", "textDescription");
        propertiesBuilder.addColumnFromField("RawMessage", "rawMessage");

        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "elevation", Integer.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "temperature", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "dewpoint", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windDirection", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windSpeed", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windGust", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "barometricPressure", Integer.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "seaLevelPressure", Integer.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "visibility", Integer.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "maxTemperatureLast24Hours", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "minTemperatureLast24Hours", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLastHour", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLast3Hours", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLast6Hours", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "relativeHumidity", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windChill", Double.class));
        mainTableColHeaders.addAll(addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "heatIndex", Double.class));


        // Add ID to cloudLayers rows
        mainTableColHeaders.add(ColumnHeader.ofLong("cloudLayers_id"));

        final JSONToTableWriterAdapterBuilder cloudLayersBuilder = new JSONToTableWriterAdapterBuilder();
        cloudLayersBuilder.autoValueMapping(false);
        cloudLayersBuilder.processArrays(true);

        final JSONToTableWriterAdapterBuilder cloudLayersBaseBuilder = new JSONToTableWriterAdapterBuilder();
        cloudLayersBaseBuilder.autoValueMapping(false);
        cloudLayersBaseBuilder.addColumnFromField("base_unitCode", "unitCode");
        cloudLayersBaseBuilder.addColumnFromField("base", "value");

        cloudLayersBuilder.addNestedField("base", cloudLayersBaseBuilder);
        cloudLayersBuilder.addColumnFromField("amount", "amount");

        DynamicTableWriter cloudLayersWriter = new DynamicTableWriter(TableHeader.of(
                // TODO: figure out something beter for these record IDs
                ColumnHeader.ofLong(JSONToTableWriterAdapter.SUBTABLE_RECORD_ID_COL),
                ColumnHeader.ofInt("base"),
                ColumnHeader.ofString("base_unitCode"),
                ColumnHeader.ofString("amount")
        ));

        propertiesBuilder.addFieldToSubTableMapping("cloudLayers", cloudLayersBuilder, cloudLayersWriter);

        mainBuilder.addNestedField("properties", propertiesBuilder);


        // Create DynamicTableWriter for the main table (now that we've defined all its columns)
        DynamicTableWriter mainObservationsWriter = new DynamicTableWriter(TableHeader.of(mainTableColHeaders.toArray(new ColumnHeader[0])));

        // Create the adapter factory for the main table
        final Function<TableWriter<?>, MessageToTableWriterAdapter<JSONToTableWriterAdapterBuilder.StringMessageHolder>> factory =
                mainBuilder.buildFactory(log);

        // Create the actual adapter:
        final MessageToTableWriterAdapter<JSONToTableWriterAdapterBuilder.StringMessageHolder> adapter = factory.apply(mainObservationsWriter);

        // Get the output tables:
        final UpdateSourceQueryTable mainTable = mainObservationsWriter.getTable();
        final UpdateSourceQueryTable cloudsTable = cloudLayersWriter.getTable();

        TableTools.show(mainTable);
        TableTools.show(cloudsTable);

        System.out.println("Consuming message!");
        adapter.consumeMessage("0",
                new JSONToTableWriterAdapterBuilder.StringMessageHolder(
                        MicroTimer.currentTimeMicros(),
                        MicroTimer.currentTimeMicros(),
                        ExampleJSON.observationsJson));
        System.out.println("Consumed message!");

        TableTools.show(mainTable);
        TableTools.show(cloudsTable);

        adapter.waitForProcessing(100000);
        adapter.cleanup();

        System.out.println("Cleaned up");

        UpdateGraphProcessor.DEFAULT.requestRefresh();
        UpdateGraphProcessor.DEFAULT.sharedLock().doLocked(() -> {
            TableTools.show(mainTable);
            TableTools.show(cloudsTable);
        });

        System.out.println("Exiting");
    }

    public static List<ColumnHeader<?>> addObservationNestedFieldBuilderAndGetColHeaders(
            JSONToTableWriterAdapterBuilder parentBuilder,
            String fieldName,
            Class<?> type
    ) {
        final String observationName = fieldName;

        JSONToTableWriterAdapterBuilder nestedBuilder = new JSONToTableWriterAdapterBuilder();
        nestedBuilder.addColumnFromField(observationName, "value");
        nestedBuilder.addColumnFromField(observationName + '_' + "unitCode", "unitCode");
        nestedBuilder.addColumnFromField(observationName + '_' + "qualityControl", "qualityControl");
        nestedBuilder.allowMissingKeys(true);
        nestedBuilder.allowNullValues(true);
        nestedBuilder.autoValueMapping(false);

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
                ColumnHeader.ofString(observationName + "_qualityControl")
        );
    }

}
