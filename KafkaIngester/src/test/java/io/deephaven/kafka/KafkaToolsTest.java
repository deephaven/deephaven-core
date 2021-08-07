package io.deephaven.kafka;

import io.deephaven.db.tables.ColumnDefinition;
import org.apache.avro.Schema;

import org.junit.Test;
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
            + "}"
        ;

    @Test
    public void testAvroSchemaWithNulls() {
        final Schema avroSchema = new Schema.Parser().parse(schemaWithNull);
        final ColumnDefinition<?>[] colDefs = KafkaTools.avroSchemaToColumnDefinitions(avroSchema);
        assertEquals(2, colDefs.length);
        assertEquals("Symbol", colDefs[0].getName());
        assertEquals(String.class, colDefs[0].getDataType());
        assertEquals("Price", colDefs[1].getName());
        assertEquals(double.class, colDefs[1].getDataType());
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
                    + "}"
            ;

    @Test
    public void testAvroSchemaWithNesting() {
        final Schema avroSchema = new Schema.Parser().parse(schemaWithNesting);
        final ColumnDefinition<?>[] colDefs = KafkaTools.avroSchemaToColumnDefinitions(avroSchema);
        assertEquals(2, colDefs.length);
        assertEquals("Symbol", colDefs[0].getName());
        assertEquals(String.class, colDefs[0].getDataType());
        assertEquals("Price", colDefs[1].getName());
        assertEquals(double.class, colDefs[1].getDataType());
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
                    + "}"
            ;

    @Test
    public void testAvroSChemaWithBasicTypesCoverage() {
        final Schema avroSchema = new Schema.Parser().parse(schemaWithBasicTypes);
        final ColumnDefinition<?>[] colDefs = KafkaTools.avroSchemaToColumnDefinitions(avroSchema);
        final int nCols = 6;
        assertEquals(nCols, colDefs.length);
        int c = 0;
        assertEquals("BooleanField", colDefs[c].getName());
        assertEquals(Boolean.class, colDefs[c++].getDataType());
        assertEquals("IntField", colDefs[c].getName());
        assertEquals(int.class, colDefs[c++].getDataType());
        assertEquals("LongField", colDefs[c].getName());
        assertEquals(long.class, colDefs[c++].getDataType());
        assertEquals("FloatField", colDefs[c].getName());
        assertEquals(float.class, colDefs[c++].getDataType());
        assertEquals("DoubleField", colDefs[c].getName());
        assertEquals(double.class, colDefs[c++].getDataType());
        assertEquals("StringField", colDefs[c].getName());
        assertEquals(String.class, colDefs[c++].getDataType());
        assertEquals(nCols, c);
    }
}
