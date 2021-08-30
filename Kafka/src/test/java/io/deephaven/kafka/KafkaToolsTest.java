package io.deephaven.kafka;

import io.deephaven.db.tables.ColumnDefinition;
import org.apache.avro.Schema;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.*;

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
        assertEquals(2, colDefs.size());
        assertEquals("Symbol", colDefs.get(0).getName());
        assertEquals(String.class, colDefs.get(0).getDataType());
        assertEquals("Price", colDefs.get(1).getName());
        assertEquals(double.class, colDefs.get(1).getDataType());
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
        assertEquals(2, colDefs.size());
        assertEquals("NestedField.Symbol", colDefs.get(0).getName());
        assertEquals(String.class, colDefs.get(0).getDataType());
        assertEquals("NestedField.Price", colDefs.get(1).getName());
        assertEquals(double.class, colDefs.get(1).getDataType());
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
        assertEquals(nCols, colDefs.size());
        int c = 0;
        assertEquals("BooleanField", colDefs.get(c).getName());
        assertEquals(Boolean.class, colDefs.get(c++).getDataType());
        assertEquals("IntField", colDefs.get(c).getName());
        assertEquals(int.class, colDefs.get(c++).getDataType());
        assertEquals("LongField", colDefs.get(c).getName());
        assertEquals(long.class, colDefs.get(c++).getDataType());
        assertEquals("FloatField", colDefs.get(c).getName());
        assertEquals(float.class, colDefs.get(c++).getDataType());
        assertEquals("DoubleField", colDefs.get(c).getName());
        assertEquals(double.class, colDefs.get(c++).getDataType());
        assertEquals("StringField", colDefs.get(c).getName());
        assertEquals(String.class, colDefs.get(c++).getDataType());
        assertEquals(nCols, c);
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
            if ("NestedFields2.NestedFields3.field4".equals(fieldName)) {
                return "field4";
            }
            return fieldName;
        };
        final List<ColumnDefinition<?>> colDefs = new ArrayList<>();
        KafkaTools.avroSchemaToColumnDefinitions(colDefs, avroSchema, mapping);
        final int nCols = 4;
        assertEquals(nCols, colDefs.size());
        int c = 0;
        assertEquals("NestedFields1.field1", colDefs.get(c).getName());
        assertEquals(int.class, colDefs.get(c++).getDataType());
        assertEquals("NestedFields1.field2", colDefs.get(c).getName());
        assertEquals(float.class, colDefs.get(c++).getDataType());
        assertEquals("NestedFields2.NestedFields3.field3", colDefs.get(c).getName());
        assertEquals(long.class, colDefs.get(c++).getDataType());
        assertEquals("field4", colDefs.get(c).getName());
        assertEquals(double.class, colDefs.get(c++).getDataType());
        assertEquals(nCols, c);
    }
}
