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
}
