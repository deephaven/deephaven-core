//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.impl;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiPredicate;

import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.apache.parquet.schema.Types.repeated;
import static org.apache.parquet.schema.Types.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

class ParquetSchemaUtilTest {

    private static final MessageType SCHEMA;

    private static final ColumnDescriptor[] NON_EXISTENT_COLUMNS = {
            new ColumnDescriptor(new String[] {"Required"}, optional(INT32).named("Required"), 0, 0),
            new ColumnDescriptor(new String[] {"Required"}, repeated(INT32).named("Required"), 0, 0),
            new ColumnDescriptor(new String[] {"Required"}, required(INT32).id(42).named("Required"), 0, 0),
            new ColumnDescriptor(new String[] {"Required2"}, required(INT32).named("Required"), 0, 0),
            new ColumnDescriptor(new String[] {"Required"}, required(INT32).named("Required2"), 0, 0),
            new ColumnDescriptor(new String[] {"Required"}, required(INT32).named("Required"), 1, 0),
            new ColumnDescriptor(new String[] {"Required"}, required(INT32).named("Required"), 0, 1),
            new ColumnDescriptor(new String[] {"Required"}, required(INT64).named("Required"), 0, 0),
            new ColumnDescriptor(new String[] {"Required"}, required(INT32).as(intType(16)).named("Required"), 0,
                    0),
            new ColumnDescriptor(new String[] {"Required"}, optional(INT32).named("Required"), 0, 0),
            new ColumnDescriptor(new String[] {}, repeated(INT32).named("Required"), 0, 0)
    };

    private static final String[][] NON_EXISTENT_LEAF_PATHS = {
            {},
            {""},
            {"root"},
            {"required"},
            {"repeated"},
            {"optional"},
            {"REQUIRED"},
            {"REPEATED"},
            {"OPTIONAL"},
            {"RequiredGroup"},
            {"RepeatedGroup"},
            {"OptionalGroup"},
            {"RequiredGroup2"},
            {"RepeatedGroup2"},
            {"OptionalGroup2"},
            {"RequiredGroup", ""},
            {"RequiredGroup", "REQUIRED"},
            {"RequiredGroup2", "REQUIRED"},
            {"RequiredGroup2", "RequiredGroup", "REQUIRED"},
            {"RequiredGroup2", "REQUIREDGROUP", "Required"},
            {"REQUIREDGROUP2", "RequiredGroup", "Required"},
            {"foo"},
            {"foo", "bar"},
            {"foo", "bar", "baz"},
            {"foo", "bar", "baz", "zip"},
    };

    static {
        final PrimitiveType required = required(INT32).named("Required");
        final PrimitiveType repeated = repeated(INT32).named("Repeated");
        final PrimitiveType optional = optional(INT32).named("Optional");
        final GroupType l1 = Types.requiredList().element(required).named("L1");
        final GroupType l2 = Types.optionalList().element(required).named("L2");
        final GroupType requiredGroup = Types.requiredGroup()
                .addFields(required, repeated, optional, l1, l2)
                .named("RequiredGroup");
        final GroupType repeatedGroup = Types.repeatedGroup()
                .addFields(required, repeated, optional, l1, l2)
                .named("RepeatedGroup");
        final GroupType optionalGroup = Types.optionalGroup()
                .addFields(required, repeated, optional, l1, l2)
                .named("OptionalGroup");
        final GroupType requiredGroup2 = Types.requiredGroup()
                .addFields(required, repeated, optional, l1, l2, requiredGroup, repeatedGroup, optionalGroup)
                .named("RequiredGroup2");
        final GroupType repeatedGroup2 = Types.repeatedGroup()
                .addFields(required, repeated, optional, l1, l2, requiredGroup, repeatedGroup, optionalGroup)
                .named("RepeatedGroup2");
        final GroupType optionalGroup2 = Types.optionalGroup()
                .addFields(required, repeated, optional, l1, l2, requiredGroup, repeatedGroup, optionalGroup)
                .named("OptionalGroup2");
        SCHEMA = Types.buildMessage()
                .addFields(required, repeated, optional, l1, l2, requiredGroup, repeatedGroup, optionalGroup,
                        requiredGroup2,
                        repeatedGroup2, optionalGroup2)
                .named("root");
    }

    @Test
    void columnsEmpty() {
        final MessageType schema = Types.buildMessage().named("root");
        final List<ColumnDescriptor> columns = ParquetSchemaUtil.columns(schema);
        assertThat(columns)
                .usingElementComparator(equalityMethod(ColumnDescriptorUtil::equals))
                .isEqualTo(schema.getColumns());
    }

    @Test
    void columns() {
        final List<ColumnDescriptor> columns = ParquetSchemaUtil.columns(SCHEMA);
        assertThat(columns)
                .usingElementComparator(equalityMethod(ColumnDescriptorUtil::equals))
                .isEqualTo(SCHEMA.getColumns());
    }

    @Test
    void columnDescriptor() {
        for (ColumnDescriptor expected : ParquetSchemaUtil.columns(SCHEMA)) {
            assertThat(ParquetSchemaUtil.columnDescriptor(SCHEMA, expected.getPath()))
                    .usingValueComparator(equalityMethod(ColumnDescriptorUtil::equals))
                    .hasValue(expected);
            // verify Parquet library has same behavior
            assertThat(SCHEMA.getColumnDescription(expected.getPath()))
                    .usingComparator(equalityMethod(ColumnDescriptorUtil::equals))
                    .isEqualTo(expected);
        }
        for (String[] nonExistentPath : NON_EXISTENT_LEAF_PATHS) {
            assertThat(ParquetSchemaUtil.columnDescriptor(SCHEMA, nonExistentPath)).isEmpty();
            // verify Parquet library has similar behavior
            try {
                SCHEMA.getColumnDescription(nonExistentPath);
                failBecauseExceptionWasNotThrown(Throwable.class);
            } catch (InvalidRecordException | ClassCastException e) {
                // good enough
            }
        }
    }

    @Test
    void contains() {
        for (ColumnDescriptor column : ParquetSchemaUtil.columns(SCHEMA)) {
            assertThat(ParquetSchemaUtil.contains(SCHEMA, column)).isTrue();
        }
        for (ColumnDescriptor column : NON_EXISTENT_COLUMNS) {
            assertThat(ParquetSchemaUtil.contains(SCHEMA, column)).isFalse();
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
