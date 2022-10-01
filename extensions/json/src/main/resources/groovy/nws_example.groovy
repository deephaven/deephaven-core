/**
 *
 * Created by rbasralian on 9/30/22
 */


import io.deephaven.engine.table.impl.UpdateSourceQueryTable
import io.deephaven.engine.table.impl.util.DynamicTableWriter
import io.deephaven.internal.log.LoggerFactory
import io.deephaven.io.logger.Logger
import io.deephaven.jsoningester.JSONToTableWriterAdapter
import io.deephaven.jsoningester.JSONToTableWriterAdapterBuilder
import io.deephaven.jsoningester.MessageToTableWriterAdapter
import io.deephaven.qst.column.header.ColumnHeader
import io.deephaven.qst.table.TableHeader
import io.deephaven.tablelogger.TableWriter
import io.deephaven.time.DateTime
import io.deephaven.time.DateTimeUtils
import io.deephaven.util.clock.MicroTimer
import org.apache.commons.compress.utils.IOUtils

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.function.Function

import io.deephaven.jsoningester.example.ComplexJsonImportTest

def featuresBuilder = new JSONToTableWriterAdapterBuilder();
featuresBuilder.autoValueMapping(false);
//featuresBuilder.processArrays(true)
featuresBuilder.addColumnFromField("FeatureId", "id");
featuresBuilder.addColumnFromField("Type", "type");

mainTableColHeaders = new ArrayList<>(32);

mainTableColHeaders.add(ColumnHeader.ofString("FeatureId"));
mainTableColHeaders.add(ColumnHeader.ofString("Type"));
mainTableColHeaders.add(ColumnHeader.of("Timestamp", DateTime.class));

// builder for 'properties', which has all the actual observations
propertiesBuilder = new JSONToTableWriterAdapterBuilder();
propertiesBuilder.autoValueMapping(false);

mainTableColHeaders.add(ColumnHeader.ofString("PropertiesId"));
mainTableColHeaders.add(ColumnHeader.ofString("Station"));
mainTableColHeaders.add(ColumnHeader.ofString("Description"));
mainTableColHeaders.add(ColumnHeader.ofString("RawMessage"));

propertiesBuilder.addColumnFromField("Station", "station");
propertiesBuilder.addColumnFromFunction("Timestamp", DateTime.class, value -> DateTimeUtils.instantToTime(Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(value.get("timestamp").textValue()))));
//        propertiesBuilder.addColumnFromField("timestamp", "Timestamp");
propertiesBuilder.addColumnFromField("Description", "textDescription");
propertiesBuilder.addColumnFromField("RawMessage", "rawMessage");
propertiesBuilder.addColumnFromField("PropertiesId", "@id");

mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "elevation", Integer.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "temperature", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "dewpoint", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windDirection", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windSpeed", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windGust", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "barometricPressure", Integer.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "seaLevelPressure", Integer.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "visibility", Integer.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "maxTemperatureLast24Hours", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "minTemperatureLast24Hours", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLastHour", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLast3Hours", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "precipitationLast6Hours", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "relativeHumidity", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "windChill", Double.class));
mainTableColHeaders.addAll(ComplexJsonImportTest.addObservationNestedFieldBuilderAndGetColHeaders(propertiesBuilder, "heatIndex", Double.class));


// Add ID to cloudLayers rows
mainTableColHeaders.add(ColumnHeader.ofLong("cloudLayers_id"));


// Add a subtable ID col (since this is nested under the outer builder)
mainTableColHeaders.add(
    // TODO: figure out something beter for these record IDs
ColumnHeader.ofLong(JSONToTableWriterAdapter.SUBTABLE_RECORD_ID_COL)
);

cloudLayersBuilder = new JSONToTableWriterAdapterBuilder();
cloudLayersBuilder.autoValueMapping(false);
cloudLayersBuilder.processArrays(true);

final JSONToTableWriterAdapterBuilder cloudLayersBaseBuilder = new JSONToTableWriterAdapterBuilder();
cloudLayersBaseBuilder.autoValueMapping(false);
cloudLayersBaseBuilder.allowNullValues(true);
//        cloudLayersBaseBuilder.allowMissingKeys(true);
cloudLayersBaseBuilder.addColumnFromField("base_unitCode", "unitCode");
cloudLayersBaseBuilder.addColumnFromField("base", "value");

cloudLayersBuilder.addNestedField("base", cloudLayersBaseBuilder);
cloudLayersBuilder.addColumnFromField("amount", "amount");

cloudLayersWriter = new DynamicTableWriter(TableHeader.of(
        // TODO: figure out something beter for these record IDs
        ColumnHeader.ofLong(JSONToTableWriterAdapter.SUBTABLE_RECORD_ID_COL),
        ColumnHeader.ofInt("base"),
        ColumnHeader.ofString("base_unitCode"),
        ColumnHeader.ofString("amount")
));

propertiesBuilder.addFieldToSubTableMapping("cloudLayers", cloudLayersBuilder, cloudLayersWriter);

// The 'properties' (i.e. actual observations) are nested under each element of the 'features' array.
featuresBuilder.addNestedField("properties", propertiesBuilder);


// Create DynamicTableWriter for the main table (now that we've defined all its columns)
mainObservationsWriter = new DynamicTableWriter(TableHeader.of(mainTableColHeaders.toArray(new ColumnHeader[0])));

outerBuilder = new JSONToTableWriterAdapterBuilder();
outerBuilder.addFieldToSubTableMapping("features", featuresBuilder, mainObservationsWriter);
outerBuilder.addColumnFromField("Type", "type")

outerObservationsWriter = new DynamicTableWriter(TableHeader.of(
        ColumnHeader.ofString("Type"),
        ColumnHeader.ofLong("features_id")
));

// Create the adapter factory for the main table
factory = outerBuilder.buildFactory(LoggerFactory.getLogger("jsonAdapter"));

// Create the actual adapter:
adapter = factory.apply(outerObservationsWriter);

// Get the output tables:
observationsTable = outerObservationsWriter.getTable();
featuresTable = mainObservationsWriter.getTable();
cloudsTable = cloudLayersWriter.getTable();


urls = [
         "https://api.weather.gov/stations/KNYC/observations",
//         "https://api.weather.gov/stations/KCOS/observations",
]

int i = 0;
for (String url : urls) {
    URL urlObj = new URL(url);
    String json;
    try (InputStream stream = urlObj.openStream()) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        json = reader.lines().collect(java.util.stream.Collectors.joining(System.lineSeparator()));
    }
    println "Sending json msg."
    adapter.consumeMessage(String.valueOf(i++), new JSONToTableWriterAdapterBuilder.StringMessageHolder(
            MicroTimer.currentTimeMicros(),
            MicroTimer.currentTimeMicros(),
            json
    ))
}