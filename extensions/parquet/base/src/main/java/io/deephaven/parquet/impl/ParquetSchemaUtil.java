//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.impl;

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
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Various improved ways of traversing {@link MessageType}.
 */
public final class ParquetSchemaUtil {

    public interface Visitor {

        /**
         * Accept a Parquet column.
         *
         * <p>
         * This represents the constituents parts of a {@link ColumnDescriptor} in an easier to consume fashion. In
         * particular, it is useful when the consumer wants to iterate the Typed-path from MessageType root to leaf
         * without needing to resort to extraneous allocation of {@link MessageType#getType(String...)} or state
         * management needed via {@link GroupType#getType(String)}. The arguments of this method can be made into a
         * {@link ColumnDescriptor} using {@link ParquetSchemaUtil#makeColumnDescriptor(Collection, PrimitiveType)}.
         *
         * @param typePath the fully typed path
         * @param primitiveType the leaf primitiveType, guaranteed to be the last element of path
         */
        void accept(Collection<Type> typePath, PrimitiveType primitiveType);
    }

    /**
     * A more efficient implementation of {@link MessageType#getColumns()}.
     */
    public static List<ColumnDescriptor> columns(MessageType schema) {
        final List<ColumnDescriptor> out = new ArrayList<>();
        walkColumnDescriptors(schema, out::add);
        return out;
    }

    /**
     * A more efficient implementation of {@link MessageType#getPaths()}.
     */
    public static List<String[]> paths(MessageType schema) {
        final List<String[]> out = new ArrayList<>();
        walk(schema, (typePath, primitiveType) -> out.add(makePath(typePath)));
        return out;
    }

    /**
     * An alternative interface for traversing the column descriptors of a Parquet {@code schema}.
     */
    public static void walkColumnDescriptors(MessageType schema, Consumer<ColumnDescriptor> consumer) {
        walk(schema, new ColumnDescriptorVisitor(consumer));
    }

    /**
     * An alternative interface for traversing the leaf fields of a Parquet {@code schema}.
     */
    public static void walk(MessageType schema, Visitor visitor) {
        walk(schema, visitor, new ArrayDeque<>());
    }

    /**
     * A more efficient implementation of {@link MessageType#getColumnDescription(String[])}.
     */
    public static Optional<ColumnDescriptor> columnDescriptor(MessageType schema, String[] path) {
        if (path.length == 0) {
            return Optional.empty();
        }
        int repeatedCount = 0;
        int notRequiredCount = 0;
        GroupType current = schema;
        for (int i = 0; i < path.length - 1; ++i) {
            if (!current.containsField(path[i])) {
                return Optional.empty();
            }
            final Type field = current.getFields().get(current.getFieldIndex(path[i]));
            if (field == null || field.isPrimitive()) {
                return Optional.empty();
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
                return Optional.empty();
            }
            final Type field = current.getFields().get(current.getFieldIndex(path[path.length - 1]));
            if (field == null || !field.isPrimitive()) {
                return Optional.empty();
            }
            primitiveType = field.asPrimitiveType();
            if (isRepeated(primitiveType)) {
                ++repeatedCount;
            }
            if (!isRequired(primitiveType)) {
                ++notRequiredCount;
            }
        }
        return Optional.of(new ColumnDescriptor(path, primitiveType, repeatedCount, notRequiredCount));
    }

    /**
     * A more efficient implementation of {@link MessageType#getColumnDescription(String[])}.
     */
    public static Optional<ColumnDescriptor> columnDescriptor(MessageType schema, List<String> path) {
        return columnDescriptor(schema, path.toArray(new String[0]));
    }

    public static ColumnDescriptor makeColumnDescriptor(Collection<Type> typePath, PrimitiveType primitiveType) {
        final String[] path = makePath(typePath);
        final int maxRep = (int) typePath.stream().filter(ParquetSchemaUtil::isRepeated).count();
        final int maxDef = (int) typePath.stream().filter(Predicate.not(ParquetSchemaUtil::isRequired)).count();
        return new ColumnDescriptor(path, primitiveType, maxRep, maxDef);
    }

    /**
     * Checks if {@code schema} contains {@code descriptor} based on
     * {@link ColumnDescriptorUtil#equals(ColumnDescriptor, ColumnDescriptor)}.
     */
    public static boolean contains(MessageType schema, ColumnDescriptor descriptor) {
        return columnDescriptor(schema, descriptor.getPath())
                .filter(cd -> ColumnDescriptorUtil.equals(descriptor, cd))
                .isPresent();
    }

    private static String[] makePath(Collection<Type> typePath) {
        return typePath.stream().map(Type::getName).toArray(String[]::new);
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
        public void accept(Collection<Type> typePath, PrimitiveType primitiveType) {
            consumer.accept(makeColumnDescriptor(typePath, primitiveType));
        }
    }
}
