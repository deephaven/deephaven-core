/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import io.deephaven.base.string.EncodingInfo;
import io.deephaven.dataobjects.persistence.DataObjectInputStream;
import io.deephaven.dataobjects.persistence.ColumnsetConversionSchema;
import io.deephaven.dataobjects.persistence.PersistentInputStream;
import io.deephaven.base.formatters.EnumFormatter;
import io.deephaven.db.tables.dbarrays.*;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.chunk.util.SimpleTypeMap;
import io.deephaven.util.codec.ObjectCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import static io.deephaven.db.tables.DataObjectUtils.readAdoString;
import static io.deephaven.db.tables.DataObjectUtils.writeAdoString;

/**
 * Column definition for all Deephaven columns.
 * Adds non-stateful functionality to DefaultColumnDefinition.
 *
 * @IncludeAll
 */
public class ColumnDefinition<TYPE> extends DefaultColumnDefinition {

    public static final ColumnDefinition[] ZERO_LENGTH_COLUMN_DEFINITION_ARRAY = new ColumnDefinition[0];

    private static final long serialVersionUID = 3656456077670712362L;

    public static final EnumFormatter COLUMN_TYPE_FORMATTER =
            new EnumFormatter(getColumnSetStatic().getColumn("ColumnType").getEnums());
    public static final EnumFormatter ENCODING_FORMATTER =
                new EnumFormatter(getColumnSetStatic().getColumn("Encoding").getEnums());

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

    public static <T> ColumnDefinition<T> ofVariableWidthCodec(
            @NotNull final String name, @NotNull final Class<T> dataType,
            @Nullable final String codecName) {
        return ofVariableWidthCodec(name, dataType, null, codecName);
    }

    public static <T> ColumnDefinition<T> ofVariableWidthCodec(
            @NotNull final String name, @NotNull final Class<T> dataType, @Nullable final Class<?> componentType,
            @Nullable final String codecName) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(dataType);
        Objects.requireNonNull(codecName);
        final ColumnDefinition<T> cd = new ColumnDefinition<>(name, dataType);
        maybeSetComponentType(cd, dataType, componentType);
        cd.setObjectCodecClass(codecName);
        return cd;
    }

    public static <T> ColumnDefinition<T> ofFixedWidthCodec(
            @NotNull final String name, @NotNull final Class<T> dataType,
            @Nullable final String codecName, @Nullable final String codecArguments, final int width) {
        return ofFixedWidthCodec(name, dataType, null, codecName, codecArguments, width);
    }

    public static <T> ColumnDefinition<T> ofFixedWidthCodec(
            @NotNull final String name, @NotNull final Class<T> dataType, @Nullable final Class<?> componentType,
            @Nullable final String codecName, @Nullable final String codecArguments, final int width) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(dataType);
        Objects.requireNonNull(codecName);
        final ColumnDefinition<T> cd = new ColumnDefinition<>(name, dataType);
        maybeSetComponentType(cd, dataType, componentType);
        cd.setObjectCodecClass(codecName);
        if (codecArguments != null) {
            cd.setObjectCodecArguments(codecArguments);
        }
        cd.setObjectWidth(width);
        return cd;
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
     * {@link DbArrayBase} class for each type.
     * Note that {@link DbBooleanArray} is deprecated, superseded by {@link DbArray}.
     */
    private static final SimpleTypeMap<Class<? extends DbArrayBase>> COMPONENT_TYPE_TO_DBARRAY_TYPE = SimpleTypeMap.create(
            DbArray.class, DbCharArray.class, DbByteArray.class, DbShortArray.class, DbIntArray.class, DbLongArray.class, DbFloatArray.class, DbDoubleArray.class, DbArray.class);

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
                if (DbArray.class.isAssignableFrom(dataType)) {
                    throw new IllegalArgumentException("Missing required component type for DbArray data type " + dataType);
                }
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

    // needed for deserialization
    public ColumnDefinition() {
    }

    private ColumnDefinition(String name, Class<TYPE> dataType) {
        this(name, dataType, COLUMNTYPE_NORMAL);
    }

    private ColumnDefinition(String name, Class<TYPE> dataType, int columnType) {
        this(name, dataType, columnType, false);
    }

    private ColumnDefinition(String name, Class<TYPE> dataType, int columnType, boolean isVarSizeString) {
        super(Objects.requireNonNull(name));
        setDataType(Objects.requireNonNull(dataType));
        setColumnType(columnType);
        setIsVarSizeString(isVarSizeString);
    }

    private ColumnDefinition(ColumnDefinition source) {
        super.copyValues(source);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public ColumnDefinition clone() {
        return new ColumnDefinition(this);
    }

    public ColumnDefinition<TYPE> withPartitioning() {
        ColumnDefinition clone = clone();
        clone.setColumnType(COLUMNTYPE_PARTITIONING);
        return clone;
    }

    public ColumnDefinition<TYPE> withGrouping() {
        ColumnDefinition clone = clone();
        clone.setColumnType(COLUMNTYPE_GROUPING);
        return clone;
    }

    public ColumnDefinition<TYPE> withNormal() {
        ColumnDefinition clone = clone();
        clone.setColumnType(COLUMNTYPE_NORMAL);
        return clone;
    }

    public <Other> ColumnDefinition<Other> withDataType(Class<Other> dataType) {
        ColumnDefinition clone = clone();
        clone.setDataType(dataType);
        return clone;
    }

    public ColumnDefinition<TYPE> withVarSizeString() {
        ColumnDefinition clone = clone();
        clone.setIsVarSizeString(true);
        return clone;
    }

    @Override
    public Class<TYPE> getDataType() {
        //noinspection unchecked
        return super.getDataType();
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
            if (getSymbolTableType() != other.getSymbolTableType()) {
                differences.add(prefix + lhs + " SymbolTableType '" + getSymbolTableType() + "' does not match " + rhs + " SymbolTableType '" + other.getSymbolTableType() + "'");
            }
            if (!Objects.equals(componentType, other.componentType)) {
                differences.add(prefix + lhs + " componentType '" + componentType + "' does not match " + rhs + " componentType '" + other.componentType + "'");
            }
            if (columnType != other.columnType) {
                differences.add(prefix + lhs + " columnType " + columnType + " does not match " + rhs + " columnType " + other.columnType);
            }
            if (getEncodingInfo(encoding) != getEncodingInfo(other.encoding)) {
                differences.add(prefix + lhs + " encoding '" + getEncodingInfo(encoding) + "' does not match " + rhs + " encoding '" + getEncodingInfo(other.encoding) + "'");
            }
            if (getObjectCodecType() != other.getObjectCodecType()) {
                differences.add(prefix + lhs + " object codec type '" + getObjectCodecType() + "' does not match " + rhs + " object codec type '" + other.getObjectCodecType() + "'");
            } else {
                if (getObjectCodecType() == ObjectCodecType.CLASS) {
                    if (!Objects.equals(getObjectCodecClass(), other.getObjectCodecClass())) {
                        differences.add(prefix + lhs + " object codec class '" + getObjectCodecClass() + "' does not match " + rhs + " object codec class '" + other.getObjectCodecClass() + "'");
                    }
                    if (!Objects.equals(getObjectCodecArguments(), other.getObjectCodecArguments())) {
                        differences.add(prefix + lhs + " object codec arguments '" + getObjectCodecArguments() + "' does not match " + rhs + " object codec arguments '" + other.getObjectCodecArguments() + "'");
                    }
                }
                if (getObjectWidth() != other.getObjectWidth()) {
                    differences.add(prefix + lhs + " object width " + getObjectWidth() + " does not match " + rhs + " object width " + other.getObjectWidth());
                }
            }
        }
    }

    public boolean equals(Object other) {
        if(!(other instanceof ColumnDefinition)) {
            return false;
        }
        final ColumnDefinition otherCD = (ColumnDefinition)other;
        return name.equals(otherCD.name) &&
                dataType.equals(otherCD.dataType) &&
                getSymbolTableType() == otherCD.getSymbolTableType() &&
                Objects.equals(componentType, otherCD.componentType) &&
                columnType == otherCD.columnType &&
                getEncodingInfo(encoding) == getEncodingInfo(otherCD.encoding) &&
                getObjectCodecType() == otherCD.getObjectCodecType() &&
                (getObjectCodecType() != ObjectCodecType.CLASS ||
                        Objects.equals(getObjectCodecClass(), otherCD.getObjectCodecClass()) &&
                        Objects.equals(getObjectCodecArguments(), otherCD.getObjectCodecArguments())) &&
                getObjectWidth() == otherCD.getObjectWidth();
    }

    public ColumnDefinition rename(String newName) {
        final ColumnDefinition renamed = clone();
        renamed.setName(newName);
        return renamed;
    }

    public enum SymbolTableType {
        NONE("None"),
        COLUMN_LOCATION("ColumnLocation");

        /**
         * Get the XML attribute value for this enum value.
         *
         * @return the XML attribute value for this enum value
         */
        public String getAttributeValue() {
            return attributeValue;
        }

        private final String attributeValue;
        SymbolTableType(String value) {
            this.attributeValue = value;
        }

        /**
         * Return the SymbolTableType with the given attribute value.
         *
         * @param attributeValue the attributeValue matching one of the enum attributeValues
         * @return the SymbolTableType with the given attribute value
         * @throws IllegalArgumentException if no match is found
         */
        public static SymbolTableType reverseLookup(String attributeValue) {
            return Arrays.stream(values()).filter(v -> v.attributeValue.equals(attributeValue)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No enum constant with attribute value '" + attributeValue + "'"));
        }
    }

    public SymbolTableType getSymbolTableType() {
        if (!CharSequence.class.isAssignableFrom(dataType) || !isDirect()) {
            return null;
        }
        if (Objects.equals(isVarSizeString, Boolean.TRUE)) {
            return SymbolTableType.NONE;
        }
        return SymbolTableType.COLUMN_LOCATION;
    }

    public EncodingInfo getEncodingInfo() {
        return getEncodingInfo(encoding);
    }

    public static EncodingInfo getEncodingInfo(final int encoding) {
        switch (encoding) {
            case ENCODING_ISO_8859_1: return EncodingInfo.ISO_8859_1;
            case ENCODING_UTF_8:      return EncodingInfo.UTF_8;
            case ENCODING_US_ASCII:   return EncodingInfo.US_ASCII;
            case ENCODING_UTF_16:     return EncodingInfo.UTF_16;
            case ENCODING_UTF_16BE:   return EncodingInfo.UTF_16BE;
            case ENCODING_UTF_16LE:   return EncodingInfo.UTF_16LE;
            default:                  return EncodingInfo.ISO_8859_1; // Should be 0, or Integer.MIN_VALUE (null).
        }
    }

    /**
     * Object codec types, allowing us to specify whether a "built-in" behavior should be used, or a codec class.
     */
    public enum ObjectCodecType {

        DEFAULT,
        SERIALIZABLE,
        EXTERNALIZABLE,
        CLASS;

        private static final EnumSet<ObjectCodecType> BUILT_IN_CODECS = EnumSet.complementOf(EnumSet.of(CLASS));

        /**
         * Determine the appropriate ObjectCodecType for the supplied "ObjectCodecClass" value.
         *
         * @param valueToLookup The name to lookup
         * @return The matching constant, or null if none was found
         */
        public static ObjectCodecType lookup(@Nullable final String valueToLookup) {
            if (valueToLookup == null) {
                return DEFAULT;
            }
            return BUILT_IN_CODECS.stream().filter(bic -> bic.name().equalsIgnoreCase(valueToLookup)).findFirst().orElse(CLASS);
        }
    }

    /**
     * Get the appropriate ObjectCodecType for this column definition.
     *
     * @return The ObjectCodecType
     */
    public ObjectCodecType getObjectCodecType() {
        return ObjectCodecType.lookup(objectCodecClass);
    }

    public boolean isFixedWidthObjectType() {
        //noinspection ConstantConditions
        assert ObjectCodec.VARIABLE_WIDTH_SENTINEL == Integer.MIN_VALUE;
        return objectWidth != ObjectCodec.VARIABLE_WIDTH_SENTINEL;
    }

    // TODO: DELETE THESE OVERRIDES (SEE NOTES IN TableDefinition)

    private static final byte STRING_ENCODING_VERSION = 1;
    private static final byte OBJECT_CODEC_VERSION = 2;
    private static final byte CURRENT_VERSION = OBJECT_CODEC_VERSION;

    static final byte MAGIC_NUMBER = (byte)0b10001111;

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ColumnsetConversionSchema conversionSchema = null;

        if (in instanceof PersistentInputStream) {
            conversionSchema = ((PersistentInputStream)in).getConversionSchema(getColumnSet().getName());
        }
        else if (in instanceof DataObjectInputStream.WrappedObjectInputStream) {
            final DataObjectInputStream childStream = ((DataObjectInputStream.WrappedObjectInputStream)in).getWObjectInputStream();

            if (childStream instanceof PersistentInputStream) {
                conversionSchema = ((PersistentInputStream)childStream).getConversionSchema(getColumnSet().getName());
            }
        }

        if (conversionSchema != null) {
            conversionSchema.readExternalADO(in, this);
       } else {
            name = readAdoString(in);

            // This read isn't using PersistentInputStream's ColumnSet conversion - need to consume stream elements for
            // the columns I've removed.
            // This is further complicated by new additions in some cases, so we've overloaded the old formula (String)
            // field as a "version number" for the externalized object -- see writeExternal() for format documentation.
            final int versionStringUtfLen = in.readUnsignedShort();
            final byte versionNumber;
            if (versionStringUtfLen != 2) {
                // This captures most possible "real" values (of which I think there are exactly 0 in use in persisted
                // ColumnDefinitions "in the wild").
                versionNumber = 0;
                in.skipBytes(versionStringUtfLen);
            } else if (in.readByte() != MAGIC_NUMBER) {
                // We have the null case ("\0", which is encoded with 2 bytes for no reason I can, understand) or a
                // "real" value of length 2.  We know it's impossible for a real, valid 2-byte UTF-8 sequence to start
                // with the magic number constant we're using.
                versionNumber = 0;
                in.skipBytes(1); // versionStringUtfLen - 1 == 1
            } else {
                // Magic number was found, it's OK to read our version number byte.
                versionNumber = in.readByte();
            }

            dataType = (Class)in.readObject();
            componentType = (Class)in.readObject();
            columnType = in.readInt();
            isVarSizeString = (Boolean)in.readObject();

            if (versionNumber < STRING_ENCODING_VERSION) {
                encoding = ENCODING_ISO_8859_1;
            } else {
                encoding = in.readInt();
            }

            if (versionNumber < OBJECT_CODEC_VERSION) {
                objectCodecClass = null;
                objectCodecArguments = null;
                objectWidth = Integer.MIN_VALUE;
            } else {
                objectCodecClass = readAdoString(in);
                objectCodecArguments = readAdoString(in);
                objectWidth = in.readInt();
            }
        }
    }

    /**
     * This is temporary, while I need backwards-compatibility for wire-format serialization of TableDefinition and
     * ColumnDefinition.
     */
    private static final ThreadLocal<Boolean> PERSISTENT_SERIALIZATION = new ThreadLocal<>();

    public static void beginPersistentSerialization() {
        PERSISTENT_SERIALIZATION.set(Boolean.TRUE);
    }

    public static void endPersistentSerialization() {
        PERSISTENT_SERIALIZATION.set(Boolean.FALSE);
    }

    public static boolean doingPersistentSerialization() {
        return PERSISTENT_SERIALIZATION.get() == Boolean.TRUE;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        writeAdoString(out, name);

        if (!doingPersistentSerialization()) {
            // PersistentOutputStream provides ColumnSets for ADO backwards-compatibility.  If we don't have one, we
            // need to write null placeholder values for "legacy" fields that I've removed from the ColumnSet/ADO.
            // We've since re-used the placeholder for the "formula" field as a single byte "version number" value.
            // This is written as 2 bytes for the "UTF length", a single byte "magic number" that *should* cause an
            // exception if something tries to parse it naively as UTF-8, and a byte to represent the version number.
            out.writeShort(2);
            out.writeByte(MAGIC_NUMBER);
            out.writeByte(CURRENT_VERSION);
        }

        out.writeObject(dataType);
        out.writeObject(componentType);
        out.writeInt(columnType);
        out.writeObject(isVarSizeString);

        out.writeInt(encoding);

        writeAdoString(out, objectCodecClass);
        writeAdoString(out, objectCodecArguments);
        out.writeInt(objectWidth);
    }
}
