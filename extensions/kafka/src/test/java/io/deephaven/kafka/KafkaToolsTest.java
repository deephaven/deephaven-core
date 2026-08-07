//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.engine.table.ColumnDefinition;
import org.apache.avro.Schema;

import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaToolsTest {
    //
    // Avro schema reference at https://avro.apache.org/docs/1.8.1/spec.html
    //

    private static final String schemaWithNull =
            "  { "
                    + "    \"type\": \"record\", "
                    + "    \"name\": \"null_schema\","
                    + "    \"namespace\": \"io.deephaven.test\","
                    + "    \"fields\" : ["
                    + "          {\"name\": \"Symbol\", \"type\": \"string\"},"
                    + "          {\"name\": \"Price\",  \"type\": [\"null\", \"double\"] }"
                    + "      ]"
                    + "}";

    @Test
    public void testAvroSchemaWithNulls() {
        final Schema avroSchema = new Schema.Parser().parse(schemaWithNull);
        final List<ColumnDefinition<?>> colDefs = new ArrayList<>();
        KafkaTools.avroSchemaToColumnDefinitions(colDefs, avroSchema);
        assertThat(colDefs.size()).isEqualTo(2);
        assertThat(colDefs.get(0).getName()).isEqualTo("Symbol");
        assertThat(colDefs.get(0).getDataType()).isEqualTo(String.class);
        assertThat(colDefs.get(1).getName()).isEqualTo("Price");
        assertThat(colDefs.get(1).getDataType()).isEqualTo(double.class);
    }

    @Test
    public void testAvroSchemaWithUTF8Strings() {
        final Schema avroSchema = new Schema.Parser().parse(schemaWithNull);
        final List<ColumnDefinition<?>> colDefs = new ArrayList<>();
        KafkaTools.avroSchemaToColumnDefinitions(colDefs, null, avroSchema, KafkaTools.DIRECT_MAPPING, true);
        assertThat(colDefs.size()).isEqualTo(2);
        assertThat(colDefs.get(0).getName()).isEqualTo("Symbol");
        assertThat(colDefs.get(0).getDataType()).isEqualTo(Utf8.class);
        assertThat(colDefs.get(1).getName()).isEqualTo("Price");
        assertThat(colDefs.get(1).getDataType()).isEqualTo(double.class);
    }

    private static final String schemaWithNesting =
            "  { "
                    + "    \"type\": \"record\", "
                    + "    \"name\": \"nested_schema\","
                    + "    \"namespace\": \"io.deephaven.test\","
                    + "    \"fields\" : ["
                    + "          {\"name\": \"NestedField\","
                    + "           \"type\": {"
                    + "                \"type\": \"record\", "
                    + "                \"name\": \"nested_record\","
                    + "                \"namespace\": \"io.deephaven.test.nested_schema\","
                    + "                \"fields\" : ["
                    + "                      {\"name\": \"Symbol\", \"type\": \"string\"},"
                    + "                      {\"name\": \"Price\",  \"type\": \"double\"}"
                    + "                  ]"
                    + "             }"
                    + "          }"
                    + "      ]"
                    + "}";

    @Test
    public void testAvroSchemaWithNesting() {
        final Schema avroSchema = new Schema.Parser().parse(schemaWithNesting);
        final List<ColumnDefinition<?>> colDefs = new ArrayList<>();
        KafkaTools.avroSchemaToColumnDefinitions(colDefs, avroSchema);
        assertThat(colDefs.size()).isEqualTo(2);
        assertThat(colDefs.get(0).getName())
                .isEqualTo("NestedField" + KafkaTools.NESTED_FIELD_COLUMN_NAME_SEPARATOR + "Symbol");
        assertThat(colDefs.get(0).getDataType()).isEqualTo(String.class);
        assertThat(colDefs.get(1).getName())
                .isEqualTo("NestedField" + KafkaTools.NESTED_FIELD_COLUMN_NAME_SEPARATOR + "Price");
        assertThat(colDefs.get(1).getDataType()).isEqualTo(double.class);
    }

    private static final String schemaWithBasicTypes =
            "  { "
                    + "    \"type\": \"record\", "
                    + "    \"name\": \"types_schema\","
                    + "    \"namespace\": \"io.deephaven.test\","
                    + "    \"fields\" : ["
                    + "          {\"name\": \"BooleanField\", \"type\": \"boolean\" },"
                    + "          {\"name\": \"IntField\",  \"type\": \"int\" },"
                    + "          {\"name\": \"LongField\",  \"type\": \"long\" },"
                    + "          {\"name\": \"FloatField\",  \"type\": \"float\" },"
                    + "          {\"name\": \"DoubleField\",  \"type\": \"double\" },"
                    + "          {\"name\": \"StringField\",  \"type\": \"string\" }"
                    + "      ]"
                    + "}";

    @Test
    public void testAvroSChemaWithBasicTypesCoverage() {
        final Schema avroSchema = new Schema.Parser().parse(schemaWithBasicTypes);
        final List<ColumnDefinition<?>> colDefs = new ArrayList<>();
        KafkaTools.avroSchemaToColumnDefinitions(colDefs, avroSchema);
        final int nCols = 6;
        assertThat(colDefs.size()).isEqualTo(nCols);
        int c = 0;
        assertThat(colDefs.get(c).getName()).isEqualTo("BooleanField");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(Boolean.class);
        assertThat(colDefs.get(c).getName()).isEqualTo("IntField");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(int.class);
        assertThat(colDefs.get(c).getName()).isEqualTo("LongField");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(long.class);
        assertThat(colDefs.get(c).getName()).isEqualTo("FloatField");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(float.class);
        assertThat(colDefs.get(c).getName()).isEqualTo("DoubleField");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(double.class);
        assertThat(colDefs.get(c).getName()).isEqualTo("StringField");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(String.class);
        assertThat(c).isEqualTo(nCols);
    }

    private static final String schemaWithMoreNesting =
            "  { "
                    + "    \"type\": \"record\", "
                    + "    \"name\": \"nested_schema\","
                    + "    \"namespace\": \"io.deephaven.test\","
                    + "    \"fields\" : ["
                    + "          {\"name\": \"NestedFields1\","
                    + "           \"type\": {"
                    + "                \"type\": \"record\", "
                    + "                \"name\": \"nested_record1\","
                    + "                \"namespace\": \"io.deephaven.test.nested_schema\","
                    + "                \"fields\" : ["
                    + "                      {\"name\": \"field1\", \"type\": \"int\"},"
                    + "                      {\"name\": \"field2\", \"type\": \"float\"}"
                    + "                  ]"
                    + "             }"
                    + "          },"
                    + "          {\"name\": \"NestedFields2\","
                    + "           \"type\": {"
                    + "                \"type\": \"record\", "
                    + "                \"name\": \"nested_record2\","
                    + "                \"namespace\": \"io.deephaven.test.nested_schema\","
                    + "                \"fields\" : ["
                    + "                      {\"name\": \"NestedFields3\","
                    + "                       \"type\": {"
                    + "                            \"type\": \"record\", "
                    + "                            \"name\": \"nested_record3\","
                    + "                            \"namespace\": \"io.deephaven.test.nested_schema\","
                    + "                            \"fields\" : ["
                    + "                                  {\"name\": \"field3\", \"type\": \"long\"},"
                    + "                                  {\"name\": \"field4\", \"type\": \"double\"}"
                    + "                              ]"
                    + "                         }"
                    + "                      }"
                    + "                  ]"
                    + "             }"
                    + "          }"
                    + "      ]"
                    + "}";

    @Test
    public void testAvroSchemaWithMoreNesting() {
        final Schema avroSchema = new Schema.Parser().parse(schemaWithMoreNesting);
        Function<String, String> mapping = (final String fieldName) -> {
            if (("NestedFields2" +
                    KafkaTools.NESTED_FIELD_NAME_SEPARATOR +
                    "NestedFields3" +
                    KafkaTools.NESTED_FIELD_NAME_SEPARATOR +
                    "field4").equals(fieldName)) {
                return "field4";
            }
            return KafkaTools.DIRECT_MAPPING.apply(fieldName);
        };
        final List<ColumnDefinition<?>> colDefs = new ArrayList<>();
        KafkaTools.avroSchemaToColumnDefinitions(colDefs, avroSchema, mapping);
        final int nCols = 4;
        assertThat(colDefs.size()).isEqualTo(nCols);
        int c = 0;
        assertThat(colDefs.get(c).getName())
                .isEqualTo("NestedFields1" + KafkaTools.NESTED_FIELD_COLUMN_NAME_SEPARATOR + "field1");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(int.class);
        assertThat(colDefs.get(c).getName())
                .isEqualTo("NestedFields1" + KafkaTools.NESTED_FIELD_COLUMN_NAME_SEPARATOR + "field2");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(float.class);
        assertThat(colDefs.get(c).getName()).isEqualTo("NestedFields2" + KafkaTools.NESTED_FIELD_COLUMN_NAME_SEPARATOR
                + "NestedFields3" + KafkaTools.NESTED_FIELD_COLUMN_NAME_SEPARATOR + "field3");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(long.class);
        assertThat(colDefs.get(c).getName()).isEqualTo("field4");
        assertThat(colDefs.get(c++).getDataType()).isEqualTo(double.class);
        assertThat(c).isEqualTo(nCols);
    }
}
