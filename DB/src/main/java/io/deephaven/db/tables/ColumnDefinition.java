/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import io.deephaven.base.Copyable;
import io.deephaven.base.formatters.EnumFormatter;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.datastructures.util.HashCodeUtil;
import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.dbarrays.DbBooleanArray;
import io.deephaven.db.tables.dbarrays.DbByteArray;
import io.deephaven.db.tables.dbarrays.DbCharArray;
import io.deephaven.db.tables.dbarrays.DbDoubleArray;
import io.deephaven.db.tables.dbarrays.DbFloatArray;
import io.deephaven.db.tables.dbarrays.DbIntArray;
import io.deephaven.db.tables.dbarrays.DbLongArray;
import io.deephaven.db.tables.dbarrays.DbShortArray;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Objects;

/**
 * Column definition for all Deephaven columns.
 */
public class ColumnDefinition<TYPE> implements Externalizable, LogOutputAppendable, Copyable<ColumnDefinition<TYPE>> {

    private static final long serialVersionUID = 3656456077670712362L;

    public static final EnumFormatter COLUMN_TYPE_FORMATTER = new EnumFormatter(new String[]{"Normal", "Grouping", "Partitioning", "Virtual"});

    public static final int COLUMNTYPE_NORMAL=1;
    public static final int COLUMNTYPE_GROUPING=2;
    public static final int COLUMNTYPE_PARTITIONING=4;
    public static final int COLUMNTYPE_VIRTUAL=8;

    public static ColumnDefinition<Boolean> ofBoolean(@NotNull final String name) {
        return new ColumnDefinition<>(name, Boolean.class);
    }

    public static ColumnDefinition<Byte> ofByte(@NotNull final String name) {
        return new ColumnDefinition<>(name, byte.class);
    }

    public static ColumnDefinition<Character> ofChar(@NotNull final String name) {
        return new ColumnDefinition<>(name, char.class);
    }

    public static ColumnDefinition<Short> ofShort(@NotNull final String name) {
        return new ColumnDefinition<>(name, short.class);
    }

    public static ColumnDefinition<Integer> ofInt(@NotNull final String name) {
        return new ColumnDefinition<>(name, int.class);
    }

    public static ColumnDefinition<Long> ofLong(@NotNull final String name) {
        return new ColumnDefinition<>(name, long.class);
    }

    public static ColumnDefinition<Float> ofFloat(@NotNull final String name) {
        return new ColumnDefinition<>(name, float.class);
    }

    public static ColumnDefinition<Double> ofDouble(@NotNull final String name) {
        return new ColumnDefinition<>(name, double.class);
    }

    public static ColumnDefinition<String> ofString(@NotNull final String name) {
        return new ColumnDefinition<>(name, String.class);
    }

    public static ColumnDefinition<DBDateTime> ofTime(@NotNull final String name) {
        return new ColumnDefinition<>(name, DBDateTime.class);
    }

    public static <T> ColumnDefinition<T> fromGenericType(@NotNull final String name, @NotNull final Class<T> dataType) {
        return fromGenericType(name, dataType, null);
    }

    public static <T> ColumnDefinition<T> fromGenericType(@NotNull final String name, @NotNull final Class<T> dataType, @Nullable final Class<?> componentType) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(dataType);
        final ColumnDefinition<T> cd = new ColumnDefinition<>(name, dataType);
        maybeSetComponentType(cd, dataType, componentType);
        return cd;
    }

    /**
     * Base component type class for each {@link DbArrayBase} type.
     * Note that {@link DbBooleanArray} is deprecated, superseded by {@link DbArray}.
     */
    private static Class<?> baseComponentTypeForDbArray(@NotNull final Class<? extends DbArrayBase> dbArrayType) {
        if (DbBooleanArray.class.isAssignableFrom(dbArrayType)) {
            return Boolean.class;
        }
        if (DbCharArray.class.isAssignableFrom(dbArrayType)) {
            return char.class;
        }
        if (DbByteArray.class.isAssignableFrom(dbArrayType)) {
            return byte.class;
        }
        if (DbShortArray.class.isAssignableFrom(dbArrayType)) {
            return short.class;
        }
        if (DbIntArray.class.isAssignableFrom(dbArrayType)) {
            return int.class;
        }
        if (DbLongArray.class.isAssignableFrom(dbArrayType)) {
            return long.class;
        }
        if (DbFloatArray.class.isAssignableFrom(dbArrayType)) {
            return float.class;
        }
        if (DbDoubleArray.class.isAssignableFrom(dbArrayType)) {
            return double.class;
        }
        if (DbArray.class.isAssignableFrom(dbArrayType)) {
            return Object.class;
        }
        throw new IllegalArgumentException("Unrecognized DbArray type " + dbArrayType);
    }

    private static void assertComponentTypeValid(@NotNull final Class<?> dataType, @Nullable final Class<?> componentType) {
        if (!DbArrayBase.class.isAssignableFrom(dataType) && !dataType.isArray()) {
            return;
        }
        if (componentType == null) {
            throw new IllegalArgumentException("Required component type not specified for data type " + dataType);
        }
        if (dataType.isArray()) {
            final Class<?> arrayComponentType = dataType.getComponentType();
            if (!arrayComponentType.isAssignableFrom(componentType)) {
                throw new IllegalArgumentException("Invalid component type " + componentType + " for array data type " + dataType);
            }
            return;
        }
        //noinspection unchecked
        final Class<?> baseComponentType = baseComponentTypeForDbArray((Class<? extends DbArrayBase>) dataType);
        if (!baseComponentType.isAssignableFrom(componentType)) {
            throw new IllegalArgumentException("Invalid component type " + componentType + " for DbArray data type " + dataType);
        }
    }

    private static Class<?> checkAndMaybeInferComponentType(@NotNull final Class<?> dataType, @Nullable final Class<?> inputComponentType) {
        if (dataType.isArray()) {
            final Class<?> arrayComponentType = dataType.getComponentType();
            if (inputComponentType == null) {
                return arrayComponentType;
            }
            if (!arrayComponentType.isAssignableFrom(inputComponentType)) {
                throw new IllegalArgumentException("Invalid component type " + inputComponentType + " for array data type " + dataType);
            }
            return inputComponentType;
        }
        if (DbArrayBase.class.isAssignableFrom(dataType)) {
            //noinspection unchecked
            final Class<?> dbArrayComponentType = baseComponentTypeForDbArray((Class<? extends DbArrayBase>) dataType);
            if (inputComponentType == null) {
                /* TODO (https://github.com/deephaven/deephaven-core/issues/817): Allow formula results returning DbArray to know component type
                 * if (DbArray.class.isAssignableFrom(dataType)) {
                 *     throw new IllegalArgumentException("Missing required component type for DbArray data type " + dataType);
                 * }
                 */
                return dbArrayComponentType;
            }
            if (!dbArrayComponentType.isAssignableFrom(inputComponentType)) {
                throw new IllegalArgumentException("Invalid component type " + inputComponentType + " for DbArray data type " + dataType);
            }
            return inputComponentType;
        }
        return inputComponentType;
    }

    private static <T> void maybeSetComponentType(@NotNull final ColumnDefinition<T> columnDefinition, @NotNull final Class<T> dataType, @Nullable Class<?> inputComponentType) {
        final Class<?> updatedComponentType = checkAndMaybeInferComponentType(dataType, inputComponentType);
        if (updatedComponentType != null) {
            columnDefinition.setComponentType(updatedComponentType);
        }
    }

    public static <T> ColumnDefinition<T> fromGenericType(String name, Class<T> dataType, int columnType, Class<?> componentType) {
        Objects.requireNonNull(dataType);
        ColumnDefinition<T> cd = new ColumnDefinition<>(name, dataType, columnType);
        if (componentType == null) {
            return cd;
        }
        cd.setComponentType(componentType);
        return cd;
    }

    public static ColumnDefinition<?> from(ColumnHeader<?> header) {
        return header.type().walk(new ColumnHeaderTranslation(header)).getOut();
    }

    private static class ColumnHeaderTranslation implements Type.Visitor, PrimitiveType.Visitor, GenericType.Visitor {

        private final ColumnHeader<?> in;
        private ColumnDefinition<?> out;

        public ColumnHeaderTranslation(ColumnHeader<?> in) {
            this.in = Objects.requireNonNull(in);
        }

        public ColumnDefinition<?> getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(PrimitiveType<?> primitiveType) {
            primitiveType.walk((PrimitiveType.Visitor) this);
        }

        @Override
        public void visit(GenericType<?> genericType) {
            genericType.walk((GenericType.Visitor) this);
        }

        @Override
        public void visit(BooleanType booleanType) {
            out = ofBoolean(in.name());
        }

        @Override
        public void visit(ByteType byteType) {
            out = ofByte(in.name());
        }

        @Override
        public void visit(CharType charType) {
            out = ofChar(in.name());
        }

        @Override
        public void visit(ShortType shortType) {
            out = ofShort(in.name());
        }

        @Override
        public void visit(IntType intType) {
            out = ofInt(in.name());
        }

        @Override
        public void visit(LongType longType) {
            out = ofLong(in.name());
        }

        @Override
        public void visit(FloatType floatType) {
            out = ofFloat(in.name());
        }

        @Override
        public void visit(DoubleType doubleType) {
            out = ofDouble(in.name());
        }

        @Override
        public void visit(StringType stringType) {
            out = ofString(in.name());
        }

        @Override
        public void visit(InstantType instantType) {
            out = ofTime(in.name());
        }

        @Override
        public void visit(CustomType<?> customType) {
            out = fromGenericType(in.name(), customType.clazz()); // todo, array types
        }
    }

    // needed for deserialization
    public ColumnDefinition() {
    }

    private ColumnDefinition(String name, Class<TYPE> dataType) {
        this(name, dataType, COLUMNTYPE_NORMAL);
    }

    private ColumnDefinition(String name, Class<TYPE> dataType, int columnType) {
        this.name = Objects.requireNonNull(name);
        setDataType(Objects.requireNonNull(dataType));
        setColumnType(columnType);
    }

    private ColumnDefinition(ColumnDefinition source) {
        copyValues(source);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public ColumnDefinition clone() {
        return new ColumnDefinition(this);
    }

    public ColumnDefinition<TYPE> withPartitioning() {
        final ColumnDefinition<TYPE> clone = safeClone();
        clone.setColumnType(COLUMNTYPE_PARTITIONING);
        return clone;
    }

    public ColumnDefinition<TYPE> withGrouping() {
        final ColumnDefinition<TYPE> clone = safeClone();
        clone.setColumnType(COLUMNTYPE_GROUPING);
        return clone;
    }

    public ColumnDefinition<TYPE> withNormal() {
        final ColumnDefinition<TYPE> clone = safeClone();
        clone.setColumnType(COLUMNTYPE_NORMAL);
        return clone;
    }

    public <Other> ColumnDefinition<Other> withDataType(Class<Other> dataType) {
        final ColumnDefinition clone = safeClone();
        clone.setDataType(dataType);
        //noinspection unchecked
        return clone;
    }

    public boolean isGrouping() {
        return (columnType == COLUMNTYPE_GROUPING);
    }

    public boolean isPartitioning() {
        return (columnType == COLUMNTYPE_PARTITIONING);
    }

    public boolean isDirect() {
        return (columnType == COLUMNTYPE_NORMAL || columnType == COLUMNTYPE_GROUPING);
    }

    /**
     * Compares two ColumnDefinitions somewhat more permissively than equals, disregarding matters of
     * storage and derivation. Checks for equality of {@code name}, {@code dataType}, and
     * {@code componentType}. As such, this method has an equivalence relation, ie
     * {@code A.isCompatible(B) == B.isCompatible(A)}.
     *
     * @param other - The ColumnDefinition to compare to.
     * @return True if the ColumnDefinition defines a column whose data is compatible with this ColumnDefinition.
     */
    public boolean isCompatible(ColumnDefinition other) {
        return this.name.equals(other.name)
            && this.dataType.equals(other.dataType)
            && this.componentType == other.componentType;
    }

    /**
     * Describes the column definition with respect to the fields that are checked in
     * {@link #isCompatible(ColumnDefinition)}.
     *
     * @return the description for compatibility
     */
    public String describeForCompatibility() {
        if (componentType == null) {
            return String.format("(%s, %s)", name, dataType);
        }
        return String.format("[%s, %s, %s]", name, dataType, componentType);
    }

    /**
     * Enumerate the differences between this ColumnDefinition, and another one.
     * Lines will be of the form "lhs attribute 'value' does not match rhs attribute 'value'.
     *
     * @param differences an array to which differences can be added
     * @param other       the ColumnDefinition under comparison
     * @param lhs         what to call "this" definition
     * @param rhs         what to call the other definition
     * @param prefix      begin each difference with this string
     */
    public void describeDifferences(@NotNull List<String> differences, @NotNull final ColumnDefinition other,
                                    @NotNull final String lhs, @NotNull final String rhs, @NotNull final String prefix) {
        if (!name.equals(other.name)) {
            differences.add(prefix + lhs + " name '" + name + "' does not match " + rhs + " name '" + other.name + "'");
        }
        if (!dataType.equals(other.dataType)) {
            differences.add(prefix + lhs + " dataType '" + dataType + "' does not match " + rhs + " dataType '" + other.dataType + "'");
        } else {
            if (!Objects.equals(componentType, other.componentType)) {
                differences.add(prefix + lhs + " componentType '" + componentType + "' does not match " + rhs + " componentType '" + other.componentType + "'");
            }
            if (columnType != other.columnType) {
                differences.add(prefix + lhs + " columnType " + columnType + " does not match " + rhs + " columnType " + other.columnType);
            }
        }
    }

    public boolean equals(final Object other) {
        if (!(other instanceof ColumnDefinition)) {
            return false;
        }
        final ColumnDefinition otherCD = (ColumnDefinition)other;
        return name.equals(otherCD.name)
                && dataType.equals(otherCD.dataType)
                && Objects.equals(componentType, otherCD.componentType)
                && columnType == otherCD.columnType
                ;
    }

    public ColumnDefinition rename(String newName) {
        final ColumnDefinition renamed = clone();
        renamed.setName(newName);
        return renamed;
    }

    private String name;
    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name=name;
    }

    private Class dataType;
    public Class getDataType() {
        return dataType;
    }

    void setDataType(Class dataType) {
        this.dataType=dataType;
    }

    private Class componentType;
    public Class getComponentType() {
        return componentType;
    }

    void setComponentType(Class componentType) {
        this.componentType=componentType;
    }

    private int columnType = Integer.MIN_VALUE;
    public int getColumnType() {
        return columnType;
    }

    void setColumnType(int columnType) {
        this.columnType=columnType;
    }

    @Override
    public void copyValues(ColumnDefinition x) {
        name = x.name;
        dataType = x.dataType;
        componentType = x.componentType;
        columnType = x.columnType;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("ColumnDefinition : ");

        builder.append("name=").append(name);
        builder.append("|dataType=").append(dataType);
        builder.append("|componentType=").append(componentType);
        builder.append("|columnType=").append(columnType);

        return builder.toString();
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        logOutput.append("ColumnDefinition : ");

        logOutput.append("name=").append(String.valueOf(name));
        logOutput.append("|dataType=").append(String.valueOf(dataType));
        logOutput.append("|componentType=").append(String.valueOf(componentType));
        logOutput.append("|columnType=").append(columnType);

        return logOutput;
    }

    public int hashCode() {
        return HashCodeUtil.toHashCode(name);
    }

    @Override
    public ColumnDefinition<TYPE> safeClone() {
        //noinspection unchecked
        return clone();
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF(); name = "\0".equals(name) ? null : name;
        dataType = (Class)in.readObject();
        componentType = (Class)in.readObject();
        columnType = in.readInt();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (name == null) { out.writeUTF("\0"); } else { out.writeUTF(name); }
        out.writeObject(dataType);
        out.writeObject(componentType);
        out.writeInt(columnType);
    }
}
