package io.deephaven.db.tables.utils;

import io.deephaven.base.verify.Require;
import junit.framework.TestCase;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import static io.deephaven.util.type.TypeUtils.*;

public class TestTypeUtils extends TestCase {

    public void testGetBoxedType() {
        Require.equals(getBoxedType(boolean.class), "getBoxedType(boolean.class)", Boolean.class, "Boolean.class");
        Require.equals(getBoxedType(byte.class), "getBoxedType(byte.class)", Byte.class, "Byte.class");
        Require.equals(getBoxedType(short.class), "getBoxedType(short.class)", Short.class, "Short.class");
        Require.equals(getBoxedType(char.class), "getBoxedType(char.class)", Character.class, "Character.class");
        Require.equals(getBoxedType(int.class), "getBoxedType(int.class)", Integer.class, "Integer.class");
        Require.equals(getBoxedType(long.class), "getBoxedType(long.class)", Long.class, "Long.class");
        Require.equals(getBoxedType(float.class), "getBoxedType(float.class)", Float.class, "Float.class");
        Require.equals(getBoxedType(double.class), "getBoxedType(double.class)", Double.class, "Double.class");

        Require.equals(getBoxedType(Boolean.class), "getBoxedType(Boolean.class)", Boolean.class, "Boolean.class");
        Require.equals(getBoxedType(Byte.class), "getBoxedType(Byte.class)", Byte.class, "Byte.class");
        Require.equals(getBoxedType(Short.class), "getBoxedType(Short.class)", Short.class, "Short.class");
        Require.equals(getBoxedType(Character.class), "getBoxedType(Character.class)", Character.class,
                "Character.class");
        Require.equals(getBoxedType(Integer.class), "getBoxedType(Integer.class)", Integer.class, "Integer.class");
        Require.equals(getBoxedType(Long.class), "getBoxedType(Long.class)", Long.class, "Long.class");
        Require.equals(getBoxedType(Float.class), "getBoxedType(Float.class)", Float.class, "Float.class");
        Require.equals(getBoxedType(Double.class), "getBoxedType(Double.class)", Double.class, "Double.class");


        Require.equals(getBoxedType(Object.class), "getBoxedType(Object.class)", Object.class, "Object.class");
        Require.equals(getBoxedType(CharSequence.class), "getBoxedType(CharSequence.class)", CharSequence.class,
                "CharSequence.class");
        Require.equals(getBoxedType(String.class), "getBoxedType(String.class)", String.class, "String.class");
    }

    public void testGetUnboxedType() {
        Require.equals(getUnboxedType(Boolean.class), "getUnboxedType(Boolean.class)", boolean.class, "boolean.class");
        Require.equals(getUnboxedType(Byte.class), "getUnboxedType(Byte.class)", byte.class, "byte.class");
        Require.equals(getUnboxedType(Short.class), "getUnboxedType(Short.class)", short.class, "short.class");
        Require.equals(getUnboxedType(Character.class), "getUnboxedType(Character.class)", char.class, "char.class");
        Require.equals(getUnboxedType(Integer.class), "getUnboxedType(Integer.class)", int.class, "int.class");
        Require.equals(getUnboxedType(Long.class), "getUnboxedType(Long.class)", long.class, "long.class");
        Require.equals(getUnboxedType(Float.class), "getUnboxedType(Float.class)", float.class, "float.class");
        Require.equals(getUnboxedType(Double.class), "getUnboxedType(Double.class)", double.class, "double.class");

        Require.equals(getUnboxedType(boolean.class), "getUnboxedType(boolean.class)", boolean.class, "boolean.class");
        Require.equals(getUnboxedType(byte.class), "getUnboxedType(byte.class)", byte.class, "byte.class");
        Require.equals(getUnboxedType(short.class), "getUnboxedType(short.class)", short.class, "short.class");
        Require.equals(getUnboxedType(char.class), "getUnboxedType(char.class)", char.class, "char.class");
        Require.equals(getUnboxedType(int.class), "getUnboxedType(int.class)", int.class, "int.class");
        Require.equals(getUnboxedType(long.class), "getUnboxedType(long.class)", long.class, "long.class");
        Require.equals(getUnboxedType(float.class), "getUnboxedType(float.class)", float.class, "float.class");
        Require.equals(getUnboxedType(double.class), "getUnboxedType(double.class)", double.class, "double.class");


        Require.eqNull(getUnboxedType(Object.class), "getUnboxedType(Object.class)");
        Require.eqNull(getUnboxedType(CharSequence.class), "getUnboxedType(CharSequence.class)");
        Require.eqNull(getUnboxedType(String.class), "getUnboxedType(String.class)");
    }

    public void testPrimitiveTypesSet() {
        Require.eq(PRIMITIVE_TYPES.size(), "PRIMITIVE_TYPES.size()", 8);
        Require.contains(PRIMITIVE_TYPES, "PRIMITIVE_TYPES", boolean.class, "boolean.class");
        Require.contains(PRIMITIVE_TYPES, "PRIMITIVE_TYPES", byte.class, "byte.class");
        Require.contains(PRIMITIVE_TYPES, "PRIMITIVE_TYPES", short.class, "short.class");
        Require.contains(PRIMITIVE_TYPES, "PRIMITIVE_TYPES", char.class, "char.class");
        Require.contains(PRIMITIVE_TYPES, "PRIMITIVE_TYPES", int.class, "int.class");
        Require.contains(PRIMITIVE_TYPES, "PRIMITIVE_TYPES", long.class, "long.class");
        Require.contains(PRIMITIVE_TYPES, "PRIMITIVE_TYPES", float.class, "float.class");
        Require.contains(PRIMITIVE_TYPES, "PRIMITIVE_TYPES", double.class, "double.class");
    }

    public void testBoxedTypesSet() {
        Require.eq(BOXED_TYPES.size(), "BOXED_TYPES.size()", 8);
        Require.contains(BOXED_TYPES, "BOXED_TYPES", Boolean.class, "Boolean.class");
        Require.contains(BOXED_TYPES, "BOXED_TYPES", Byte.class, "Byte.class");
        Require.contains(BOXED_TYPES, "BOXED_TYPES", Short.class, "Short.class");
        Require.contains(BOXED_TYPES, "BOXED_TYPES", Character.class, "Character.class");
        Require.contains(BOXED_TYPES, "BOXED_TYPES", Integer.class, "Integer.class");
        Require.contains(BOXED_TYPES, "BOXED_TYPES", Long.class, "Long.class");
        Require.contains(BOXED_TYPES, "BOXED_TYPES", Float.class, "Float.class");
        Require.contains(BOXED_TYPES, "BOXED_TYPES", Double.class, "Double.class");
    }

    public void testTypesSetOrdering() {
        final Class[] primitiveTypes = PRIMITIVE_TYPES.toArray(new Class[0]);
        final int charIndex = ArrayUtils.indexOf(primitiveTypes, char.class);
        final int byteIndex = ArrayUtils.indexOf(primitiveTypes, byte.class);
        final int shortIndex = ArrayUtils.indexOf(primitiveTypes, short.class);
        final int intIndex = ArrayUtils.indexOf(primitiveTypes, int.class);
        final int longIndex = ArrayUtils.indexOf(primitiveTypes, long.class);
        final int floatIndex = ArrayUtils.indexOf(primitiveTypes, float.class);
        final int doubleIndex = ArrayUtils.indexOf(primitiveTypes, double.class);
        final int booleanIndex = ArrayUtils.indexOf(primitiveTypes, boolean.class);

        Require.geqZero(charIndex, "charIndex");
        Require.geqZero(byteIndex, "byteIndex");
        Require.geqZero(shortIndex, "shortIndex");
        Require.geqZero(intIndex, "intIndex");
        Require.geqZero(longIndex, "longIndex");
        Require.geqZero(floatIndex, "floatIndex");
        Require.geqZero(doubleIndex, "doubleIndex");
        Require.geqZero(booleanIndex, "booleanIndex");

        // Ensure types are ordered from narrowest to widest
        Require.lt(byteIndex, "byteIndex", shortIndex, "shortIndex");
        Require.lt(shortIndex, "shortIndex", charIndex, "charIndex");
        Require.lt(charIndex, "charIndex", intIndex, "intIndex");
        Require.lt(intIndex, "intIndex", longIndex, "longIndex");
        Require.lt(longIndex, "longIndex", floatIndex, "floatIndex");
        Require.lt(floatIndex, "floatIndex", doubleIndex, "doubleIndex");
        Require.lt(doubleIndex, "doubleIndex", booleanIndex, "booleanIndex");

        // Ensure primitive types and boxed types have the same ordering
        Require.requirement(
                Arrays.equals(PRIMITIVE_TYPES.toArray(),
                        BOXED_TYPES.stream().map(io.deephaven.util.type.TypeUtils::getUnboxedType).toArray()),
                "Arrays.equals(PRIMITIVE_TYPES.toArray(), BOXED_TYPES.stream().map(TypeUtils::getUnboxedType).toArray())");
    }

    public void testIsType() {
        assertFalse(io.deephaven.util.type.TypeUtils.isPrimitiveNumeric(DBDateTime.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isPrimitiveNumeric(Date.class));
        assertTrue(io.deephaven.util.type.TypeUtils.isPrimitiveNumeric(int.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isPrimitiveNumeric(Double.class));

        assertFalse(io.deephaven.util.type.TypeUtils.isBoxedNumeric(DBDateTime.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isBoxedNumeric(Date.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isBoxedNumeric(int.class));
        assertTrue(io.deephaven.util.type.TypeUtils.isBoxedNumeric(Double.class));

        assertFalse(io.deephaven.util.type.TypeUtils.isNumeric(DBDateTime.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isNumeric(Date.class));
        assertTrue(io.deephaven.util.type.TypeUtils.isNumeric(int.class));
        assertTrue(io.deephaven.util.type.TypeUtils.isNumeric(Double.class));

        assertFalse(io.deephaven.util.type.TypeUtils.isCharacter(DBDateTime.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isCharacter(Date.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isCharacter(int.class));
        assertTrue(io.deephaven.util.type.TypeUtils.isCharacter(char.class));
        assertTrue(io.deephaven.util.type.TypeUtils.isCharacter(Character.class));

        assertFalse(io.deephaven.util.type.TypeUtils.isPrimitiveChar(DBDateTime.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isPrimitiveChar(Date.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isPrimitiveChar(int.class));
        assertTrue(io.deephaven.util.type.TypeUtils.isPrimitiveChar(char.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isPrimitiveChar(Character.class));

        assertFalse(io.deephaven.util.type.TypeUtils.isBoxedChar(DBDateTime.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isBoxedChar(Date.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isBoxedChar(int.class));
        assertFalse(io.deephaven.util.type.TypeUtils.isBoxedChar(char.class));
        assertTrue(io.deephaven.util.type.TypeUtils.isBoxedChar(Character.class));
    }

    public void testObjectToString() throws IOException {
        assertNull(io.deephaven.util.type.TypeUtils.objectToString(null)); // null input
        assertEquals("STRING", io.deephaven.util.type.TypeUtils.objectToString("STRING")); // non null input
    }
}
