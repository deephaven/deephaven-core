//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetUtilTest {

    private static final MessageType SCHEMA;

    static {
        final PrimitiveType required = Types.required(PrimitiveTypeName.INT32).named("Required");
        final PrimitiveType repeated = Types.repeated(PrimitiveTypeName.INT32).named("Repeated");
        final PrimitiveType optional = Types.optional(PrimitiveTypeName.INT32).named("Optional");
        final GroupType requiredGroup = Types.requiredGroup()
                .addFields(required, repeated, optional)
                .named("RequiredGroup");
        final GroupType repeatedGroup = Types.repeatedGroup()
                .addFields(required, repeated, optional)
                .named("RepeatedGroup");
        final GroupType optionalGroup = Types.optionalGroup()
                .addFields(required, repeated, optional)
                .named("OptionalGroup");
        final GroupType requiredGroup2 = Types.requiredGroup()
                .addFields(required, repeated, optional, requiredGroup, repeatedGroup, optionalGroup)
                .named("RequiredGroup2");
        final GroupType repeatedGroup2 = Types.repeatedGroup()
                .addFields(required, repeated, optional, requiredGroup, repeatedGroup, optionalGroup)
                .named("RepeatedGroup2");
        final GroupType optionalGroup2 = Types.optionalGroup()
                .addFields(required, repeated, optional, requiredGroup, repeatedGroup, optionalGroup)
                .named("OptionalGroup2");
        SCHEMA = Types.buildMessage()
                .addFields(required, repeated, optional, requiredGroup, repeatedGroup, optionalGroup, requiredGroup2,
                        repeatedGroup2, optionalGroup2)
                .named("root");
    }

    @Test
    public void getColumnsEmpty() {
        final MessageType schema = Types.buildMessage().named("root");
        final List<ColumnDescriptor> columns = ParquetUtil.getColumns(schema);
        assertThat(columns)
                .usingElementComparator(equalityMethod(ParquetUtil::columnDescriptorEquals))
                .isEqualTo(schema.getColumns());
    }

    @Test
    public void getColumns() {
        final List<ColumnDescriptor> columns = ParquetUtil.getColumns(SCHEMA);
        assertThat(columns)
                .usingElementComparator(equalityMethod(ParquetUtil::columnDescriptorEquals))
                .isEqualTo(SCHEMA.getColumns());

    }

    @Test
    public void getColumnDescriptor() {
        for (ColumnDescriptor expected : ParquetUtil.getColumns(SCHEMA)) {
            assertThat(ParquetUtil.getColumnDescriptor(SCHEMA, expected.getPath()))
                    .usingComparator(equalityMethod(ParquetUtil::columnDescriptorEquals))
                    .isEqualTo(expected);
        }
    }

    @Test
    public void contains() {
        for (ColumnDescriptor column : ParquetUtil.getColumns(SCHEMA)) {
            assertThat(ParquetUtil.contains(SCHEMA, column)).isTrue();
        }
        assertThat(ParquetUtil.contains(SCHEMA, new ColumnDescriptor(new String[] {"Required"},
                Types.required(PrimitiveTypeName.INT32).named("Required"), 0, 0))).isTrue();
        for (ColumnDescriptor column : new ColumnDescriptor[] {
                new ColumnDescriptor(new String[] {"Required"},
                        Types.optional(PrimitiveTypeName.INT32).named("Required"), 0, 0),
                new ColumnDescriptor(new String[] {"Required"},
                        Types.repeated(PrimitiveTypeName.INT32).named("Required"), 0, 0),
                new ColumnDescriptor(new String[] {"Required"},
                        Types.required(PrimitiveTypeName.INT32).id(42).named("Required"), 0, 0),
                new ColumnDescriptor(new String[] {"Required2"},
                        Types.required(PrimitiveTypeName.INT32).named("Required"), 0, 0),
                new ColumnDescriptor(new String[] {"Required"},
                        Types.required(PrimitiveTypeName.INT32).named("Required2"), 0, 0),
                new ColumnDescriptor(new String[] {"Required"},
                        Types.required(PrimitiveTypeName.INT32).named("Required"), 1, 0),
                new ColumnDescriptor(new String[] {"Required"},
                        Types.required(PrimitiveTypeName.INT32).named("Required"), 0, 1),
                new ColumnDescriptor(new String[] {"Required"},
                        Types.required(PrimitiveTypeName.INT64).named("Required"), 0, 0),
                new ColumnDescriptor(new String[] {"Required"},
                        Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(16)).named("Required"),
                        0, 0),
                new ColumnDescriptor(new String[] {"Required"},
                        Types.optional(PrimitiveTypeName.INT32).named("Required"), 0, 0),
                new ColumnDescriptor(new String[] {}, Types.repeated(PrimitiveTypeName.INT32).named("Required"), 0, 0)
        }) {
            assertThat(ParquetUtil.contains(SCHEMA, column)).isFalse();
        }
    }

    /**
     * This is not a valid comparator; it may only be used with assertJ for equality purposes and not comparison
     * purposes. See <a href="https://github.com/assertj/assertj/issues/3678">Support specialized equality methods</a>
     */
    private static <T> Comparator<T> equalityMethod(BiPredicate<T, T> predicate) {
        // noinspection ComparatorMethodParameterNotUsed
        return (o1, o2) -> predicate.test(o1, o2) ? 0 : -1;
    }
}
