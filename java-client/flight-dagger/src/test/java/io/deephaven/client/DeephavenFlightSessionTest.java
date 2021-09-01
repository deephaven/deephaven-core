package io.deephaven.client;

import io.deephaven.api.TableOperations;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableCreatorImpl;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class DeephavenFlightSessionTest extends DeephavenFlightSessionTestBase {
    public static <T extends TableOperations<T, T>> T i32768(TableCreator<T> c) {
        return c.emptyTable(32768).view("I=i");
    }

    @Test
    public void getSchema() throws Exception {
        final TableSpec table = i32768(TableCreatorImpl.INSTANCE);
        try (final TableHandle handle = flightSession.session().execute(table)) {
            final Schema schema = flightSession.getSchema(handle.export());
            final Schema expected = new Schema(Collections.singletonList(
                    new Field("I", new FieldType(true, MinorType.INT.getType(), null, null), Collections.emptyList())));
            assertThat(metadataLess(schema)).isEqualTo(expected);
        }
    }

    @Test
    public void getStream() throws Exception {
        final TableSpec table = i32768(TableCreatorImpl.INSTANCE);
        try (
                final TableHandle handle = flightSession.session().execute(table);
                final FlightStream stream = flightSession.getStream(handle.export())) {
            System.out.println(stream.getSchema());
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
        }
    }

    private static Schema metadataLess(Schema schema) {
        return new Schema(
                schema.getFields().stream().map(DeephavenFlightSessionTest::metadataLess).collect(Collectors.toList()),
                null);
    }

    private static Field metadataLess(Field field) {
        return new Field(field.getName(), metadataLess(field.getFieldType()), field.getChildren());
    }

    private static FieldType metadataLess(FieldType fieldType) {
        return new FieldType(fieldType.isNullable(), fieldType.getType(), fieldType.getDictionary(), null);
    }
}
