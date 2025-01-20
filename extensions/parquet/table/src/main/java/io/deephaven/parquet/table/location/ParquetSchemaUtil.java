//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.base.verify.Assert;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

final class ParquetSchemaUtil {

    interface Visitor {

        /**
         * Accept a Parquet column.
         *
         * <p>
         * This represents the constituents parts of a {@link ColumnDescriptor} in an easier to consume fashion. In
         * particular, it is useful when the consumer wants to iterate the Typed-path from MessageType root to leaf
         * without needing to resort to extraneous allocation of {@link MessageType#getType(String...)} or state
         * management needed via {@link GroupType#getType(String)}. The arguments of this method can be made into a
         * {@link ColumnDescriptor} using {@link #makeColumnDescriptor(Collection, PrimitiveType)}.
         *
         * @param path the full path
         * @param primitiveType the leaf primitiveType, guaranteed to be the last element of path
         */
        void accept(Collection<Type> path, PrimitiveType primitiveType);
    }

    /**
     * A more efficient implementation of {@link MessageType#getColumns()}.
     */
    static List<ColumnDescriptor> getColumns(MessageType schema) {
        final List<ColumnDescriptor> out = new ArrayList<>();
        walkColumnDescriptors(schema, out::add);
        return out;
    }

    static void walkColumnDescriptors(MessageType type, Consumer<ColumnDescriptor> consumer) {
        walk(type, new ColumnDescriptorVisitor(consumer));
    }

    /**
     * An alternative interface for traversing the leaf fields of a Parquet schema.
     */
    static void walk(MessageType type, Visitor visitor) {
        walk(type, visitor, new ArrayDeque<>());
    }

    /**
     * A more efficient implementation of {@link MessageType#getColumnDescription(String[])}
     */
    static ColumnDescriptor getColumnDescriptor(MessageType schema, String[] path) {
        if (path.length == 0) {
            return null;
        }
        int repeatedCount = 0;
        int notRequiredCount = 0;
        GroupType current = schema;
        for (int i = 0; i < path.length - 1; ++i) {
            if (!current.containsField(path[i])) {
                return null;
            }
            final Type field = current.getFields().get(current.getFieldIndex(path[i]));
            if (field == null || field.isPrimitive()) {
                return null;
            }
            current = field.asGroupType();
            if (isRepeated(current)) {
                ++repeatedCount;
            }
            if (!isRequired(current)) {
                ++notRequiredCount;
            }
        }
        final PrimitiveType primitiveType;
        {
            if (!current.containsField(path[path.length - 1])) {
                return null;
            }
            final Type field = current.getFields().get(current.getFieldIndex(path[path.length - 1]));
            if (field == null || !field.isPrimitive()) {
                return null;
            }
            primitiveType = field.asPrimitiveType();
            if (isRepeated(primitiveType)) {
                ++repeatedCount;
            }
            if (!isRequired(primitiveType)) {
                ++notRequiredCount;
            }
        }
        return new ColumnDescriptor(path, primitiveType, repeatedCount, notRequiredCount);
    }

    static ColumnDescriptor makeColumnDescriptor(Collection<Type> path, PrimitiveType primitiveType) {
        final String[] namePath = path.stream().map(Type::getName).toArray(String[]::new);
        final int maxRep = (int) path.stream().filter(ParquetSchemaUtil::isRepeated).count();
        final int maxDef = (int) path.stream().filter(Predicate.not(ParquetSchemaUtil::isRequired)).count();
        return new ColumnDescriptor(namePath, primitiveType, maxRep, maxDef);
    }

    static boolean columnDescriptorEquals(ColumnDescriptor a, ColumnDescriptor b) {
        return a.equals(b)
                && a.getPrimitiveType().equals(b.getPrimitiveType())
                && a.getMaxRepetitionLevel() == b.getMaxRepetitionLevel()
                && a.getMaxDefinitionLevel() == b.getMaxDefinitionLevel();
    }

    static boolean contains(MessageType schema, ColumnDescriptor descriptor) {
        final ColumnDescriptor cd = getColumnDescriptor(schema, descriptor.getPath());
        if (cd == null) {
            return false;
        }
        return columnDescriptorEquals(descriptor, cd);
    }

    private static void walk(Type type, Visitor visitor, Deque<Type> stack) {
        if (type.isPrimitive()) {
            visitor.accept(stack, type.asPrimitiveType());
            return;
        }
        walk(type.asGroupType(), visitor, stack);
    }

    private static void walk(GroupType type, Visitor visitor, Deque<Type> stack) {
        for (final Type field : type.getFields()) {
            Assert.eqTrue(stack.offerLast(field), "stack.offerLast(field)");
            walk(field, visitor, stack);
            Assert.eq(stack.pollLast(), "stack.pollLast()", field, "field");
        }
    }

    private static boolean isRepeated(Type x) {
        return x.isRepetition(Repetition.REPEATED);
    }

    private static boolean isRequired(Type x) {
        return x.isRepetition(Repetition.REQUIRED);
    }

    private static class ColumnDescriptorVisitor implements Visitor {

        private final Consumer<ColumnDescriptor> consumer;

        public ColumnDescriptorVisitor(Consumer<ColumnDescriptor> consumer) {
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void accept(Collection<Type> path, PrimitiveType primitiveType) {
            consumer.accept(makeColumnDescriptor(path, primitiveType));
        }
    }
}
