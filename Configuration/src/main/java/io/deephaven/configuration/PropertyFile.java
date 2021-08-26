/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.configuration;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.io.logger.Logger;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.BitSet;
import java.util.*;
import java.util.regex.Matcher;

import java.util.regex.Pattern;

public class PropertyFile {
    protected final Logger log;
    protected Properties properties = new Properties();

    /**
     * Creates an empty PropertyFile
     */
    public PropertyFile() {
        this.log = LoggerFactory.getLogger(PropertyFile.class);
    }

    /**
     * Create a PropertyFile and load properties from the given file.
     *
     * @param filename name of file to load properties from
     * @param log Logger for error/warning messages, or null if not logging
     * @param fatal If the file can't be read and fatal is true -> throw exception
     */
    public PropertyFile(String filename, Logger log, boolean fatal) {
        this.log = log;
        try {
            properties.load(new FileInputStream(filename));
        } catch (IOException e) {
            if (fatal) {
                throw new PropertyException("Error reading engine params file", e);
            } else {
                if (log != null) {
                    log.warn("Error reading engine params file: " + e);
                }
            }
        }
    }

    /**
     * Return the Properties object loaded by this property file.
     * 
     * @return the Properties object.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Collect all of the properties in this property file that begin with a given prefix.
     * 
     * @return a new Properties object containing the selected properties, with the prefix removed.
     */
    public Properties getProperties(String prefix) {
        Properties result = new Properties();
        for (Object key : properties.keySet()) {
            if (!(key instanceof String)) {
                continue;
            }
            String s = (String) key;
            if (s.startsWith(prefix)) {
                String value = properties.getProperty(s);
                result.setProperty(s.substring(prefix.length()), value);
            }
        }
        return result;
    }

    /**
     * Sets the value of a given property
     *
     * @param key an identifier for the property
     * @param value the value for the property
     * @return the previous value of the property
     */
    public String setProperty(String key, String value) {
        if (null == value) {
            return (String) properties.remove(key);
        } else {
            return (String) properties.setProperty(key, value);
        }
    }

    /**
     * Gets the value of the given property, aborting if the value is not specified in the system config files. Note
     * that it is by design that there is no overloaded method taking a default value. Rather than scattering default
     * values through all the source files, all properties should be in one config file (as much as possible). Put
     * default values in <U>common.prop</U>.
     */
    public @NotNull String getProperty(String propertyName) {
        String result = properties.getProperty(propertyName);
        if (null == result) {
            throw new PropertyException("Missing property in config file: " + propertyName);
        }
        return result;
    }

    public char getChar(String propertyName) {
        final String propertyValue = getProperty(propertyName).trim();
        return propertyValue.length() > 0 ? propertyValue.charAt(0) : '\0';
    }

    public int getInteger(String propertyName) {
        return Integer.parseInt(getProperty(propertyName).trim());
    }

    public int getPositiveInteger(String propertyName) {
        return Require.gtZero(getInteger(propertyName), propertyName);
    }

    public short getShort(String propertyName) {
        return Short.parseShort(getProperty(propertyName).trim());
    }

    public long getLong(String propertyName) {
        return Long.parseLong(getProperty(propertyName).trim());
    }

    public double getDouble(String propertyName) {
        return Double.parseDouble(getProperty(propertyName).trim());
    }

    public boolean getBoolean(String propertyName) {
        return Boolean.valueOf(getProperty(propertyName).trim());
    }

    public boolean getBooleanWithDefault(String propertyName, final boolean defaultValue) {
        if (!hasProperty(propertyName)) {
            return defaultValue;
        }
        return getBoolean(propertyName);
    }

    public long getLongWithDefault(String propertyName, final long defaultValue) {
        if (!hasProperty(propertyName)) {
            return defaultValue;
        }
        return getLong(propertyName);
    }

    public short getShortWithDefault(String propertyName, final short defaultValue) {
        if (!hasProperty(propertyName)) {
            return defaultValue;
        }
        return getShort(propertyName);
    }

    public int getIntegerWithDefault(String propertyName, final int defaultValue) {
        if (!hasProperty(propertyName)) {
            return defaultValue;
        }
        return getInteger(propertyName);
    }

    public int getPossibleIntegerWithDefault(final int defaultValue, final String... possiblePropertyNames) {
        for (final String propertyName : possiblePropertyNames) {
            if (hasProperty(propertyName)) {
                return getInteger(propertyName);
            }
        }
        return defaultValue;
    }

    public String getStringWithDefault(final String propertyName, final String defaultValue) {
        if (!hasProperty(propertyName)) {
            return defaultValue;
        }
        return getProperty(propertyName);
    }

    public String getPossibleStringWithDefault(final String defaultValue, final String... possiblePropertyNames) {
        for (final String propertyName : possiblePropertyNames) {
            if (hasProperty(propertyName)) {
                return getProperty(propertyName);
            }
        }
        return defaultValue;
    }

    public double getDoubleWithDefault(String propertyName, final double defaultValue) {
        if (!hasProperty(propertyName)) {
            return defaultValue;
        }
        return getDouble(propertyName);
    }

    private interface Parser {
        java.lang.Object parse(String s) throws NumberFormatException;

        String typeName();
    }
    private static class BooleanParser implements Parser {
        public java.lang.Boolean parse(String s) throws NumberFormatException {
            return Boolean.valueOf(s);
        }

        public String typeName() {
            return "boolean";
        }
    }
    private static class IntegerParser implements Parser {
        public java.lang.Integer parse(String s) throws NumberFormatException {
            return Integer.valueOf(s);
        }

        public String typeName() {
            return "int";
        }
    }
    private static class ShortParser implements Parser {
        public java.lang.Short parse(String s) throws NumberFormatException {
            return Short.valueOf(s);
        }

        public String typeName() {
            return "short";
        }
    }
    private static class LongParser implements Parser {
        public java.lang.Long parse(String s) throws NumberFormatException {
            return Long.valueOf(s);
        }

        public String typeName() {
            return "long";
        }
    }
    private static class DoubleParser implements Parser {
        public java.lang.Double parse(String s) throws NumberFormatException {
            return Double.valueOf(s);
        }

        public String typeName() {
            return "double";
        }
    }

    private static class StringParser implements Parser {
        public String parse(String s) throws NumberFormatException {
            return s;
        }

        public String typeName() {
            return "String";
        }
    }

    private <T extends Parser> java.lang.Object get(T parser, String propertyName,
            Logger logger, String logPrefix) {
        String propStringValue = properties.getProperty(propertyName);
        if (propStringValue == null) {
            String msg = "property " + propertyName + " is missing";
            logger.error(logPrefix + ": " + msg);
            throw new PropertyException(msg);
        }
        try {
            java.lang.Object propValue = parser.parse(propStringValue);
            logger.info(logPrefix + ": property " + propertyName + " = " + propValue);
            return propValue;
        } catch (NumberFormatException e) {
            String msg = "property " + propertyName + " string value " + propStringValue
                    + " couldn't be parsed as " + parser.typeName();
            logger.error(logPrefix + ": " + msg);
            throw new PropertyException(msg);
        }
    }

    private static final Parser booleanParser = new BooleanParser();
    private static final Parser integerParser = new IntegerParser();
    private static final Parser shortParser = new ShortParser();
    private static final Parser longParser = new LongParser();
    private static final Parser doubleParser = new DoubleParser();
    private static final Parser stringParser = new StringParser();

    public boolean getBoolean(String propertyName, Logger logger, String logPrefix) {
        Object result = get(booleanParser, propertyName, logger, logPrefix);
        return ((Boolean) result);
    }

    public int getInteger(String propertyName, Logger logger, String logPrefix) {
        Object result = get(integerParser, propertyName, logger, logPrefix);
        return ((Integer) result);
    }

    public short getShort(String propertyName, Logger logger, String logPrefix) {
        Object result = get(shortParser, propertyName, logger, logPrefix);
        return ((Short) result);
    }

    public long getLong(String propertyName, Logger logger, String logPrefix) {
        Object result = get(longParser, propertyName, logger, logPrefix);
        return ((Long) result);
    }

    public double getDouble(String propertyName, Logger logger, String logPrefix) {
        Object result = get(doubleParser, propertyName, logger, logPrefix);
        return ((Double) result);
    }

    public String getString(String propertyName, Logger logger, String logPrefix) {
        Object result = get(stringParser, propertyName, logger, logPrefix);
        return ((String) result);
    }

    // Convenience methods... if only java had default parameter values...
    public boolean getBooleanForClass(Class c, String propertyLast) {
        return getBoolean(c.getSimpleName() + "." + propertyLast, log, c.getName());
    }

    public boolean getBooleanForClassWithDefault(Class c, String propertyLast, final boolean defaultValue) {
        final String prop = c.getSimpleName() + "." + propertyLast;
        if (!hasProperty(prop)) {
            return defaultValue;
        }
        return getBoolean(prop, log, c.getName());
    }

    public int getIntegerForClass(Class c, String propertyLast) {
        return getInteger(c.getSimpleName() + "." + propertyLast, log, c.getName());
    }

    public int getIntegerForClassWithDefault(final Class c, final String propertyLast, final int defaultValue) {
        final String prop = c.getSimpleName() + "." + propertyLast;
        if (!hasProperty(prop)) {
            return defaultValue;
        }
        return getInteger(prop, log, c.getName());
    }

    public long getShortForClass(Class c, String propertyLast) {
        return getShort(c.getSimpleName() + "." + propertyLast, log, c.getName());
    }

    public long getLongForClass(Class c, String propertyLast) {
        return getLong(c.getSimpleName() + "." + propertyLast, log, c.getName());
    }

    public long getLongForClassWithDefault(final Class c, final String propertyLast, final long defaultValue) {
        final String prop = c.getSimpleName() + "." + propertyLast;
        if (!hasProperty(prop)) {
            return defaultValue;
        }
        return getLong(prop, log, c.getName());
    }

    public double getDoubleForClass(Class c, String propertyLast) {
        return getDouble(c.getSimpleName() + "." + propertyLast, log, c.getName());
    }

    public double getDoubleForClassWithDefault(Class c, String propertyLast, final double defaultValue) {
        final String prop = c.getSimpleName() + "." + propertyLast;
        if (!hasProperty(prop)) {
            return defaultValue;
        }
        return getDouble(prop, log, c.getName());
    }

    public String getStringForClass(Class c, String propertyLast) {
        return getString(c.getSimpleName() + "." + propertyLast, log, c.getName());
    }

    public boolean hasProperty(String propertyName) {
        return (properties.getProperty(propertyName) != null);
    }

    public String getStringForClassWithDefault(Class c, String propertyLast, String defaultValue) {
        final String prop = c.getSimpleName() + "." + propertyLast;
        if (!hasProperty(prop)) {
            return defaultValue;
        }
        return getString(prop, log, c.getName());
    }

    public TIntHashSet getIntHashSetForClass(Class c, String propertyLast) {
        return getIntHashSetFromProperty(c.getSimpleName() + "." + propertyLast);
    }

    public int[] getIntArrayForClass(Class c, String propertyLast) {
        return getIntegerArray(c.getSimpleName() + "." + propertyLast);
    }

    public void getClassParams(final Logger log, final Class c, final String instanceStr,
            final Object obj, final int desiredMods) {
        Class paramClass = obj.getClass();
        Field[] fields = paramClass.getDeclaredFields();
        final String propBase = c.getSimpleName();
        final String errMsg = "(getClassParams) class " + propBase + " property error.";
        for (Field f : fields) {
            if ((f.getModifiers() & desiredMods) == 0) {
                throw new PropertyException(errMsg,
                        new PropertyException("Field with wrong modifiers " + f.toString()));
            }
            final String fieldName = f.getName();
            final String s = (instanceStr == null || instanceStr.length() == 0) ? "" : (instanceStr + ".");
            final String propName = propBase + "." + s + fieldName;
            try {
                final String value = getProperties().getProperty(propName);
                String source;
                if (value == null) {
                    throw new PropertyException("null value for property " + propName);
                } else {
                    source = "file";
                    // Setting field accessibility should be allowed by our code, but not from outside classes, so
                    // this should be privileged.
                    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        f.setAccessible(true);
                        return null;
                    });
                    final String fieldTypeName = f.getType().getName();
                    if (fieldTypeName.equals("java.lang.String")) {
                        f.set(obj, value.trim());
                    } else if (fieldTypeName.equals("char")) {
                        f.setChar(obj, value.charAt(0));
                    } else if (fieldTypeName.equals("byte")) {
                        f.setByte(obj, Byte.parseByte(value));
                    } else if (fieldTypeName.equals("boolean")) {
                        f.setBoolean(obj, Boolean.parseBoolean(value));
                    } else if (fieldTypeName.equals("short")) {
                        f.setShort(obj, Short.parseShort(value));
                    } else if (fieldTypeName.equals("int")) {
                        f.setInt(obj, Integer.parseInt(value));
                    } else if (fieldTypeName.equals("long")) {
                        f.setLong(obj, Long.parseLong(value));
                    } else if (fieldTypeName.equals("float")) {
                        f.setFloat(obj, Float.parseFloat(value));
                    } else if (fieldTypeName.equals("double")) {
                        f.setDouble(obj, Double.parseDouble(value));
                    } else {
                        throw new PropertyException("Type not supported for property " + propName);
                    }
                }
                if (log != null)
                    log.info(">>>" + propName + " = " + f.get(obj).toString() + " (" + source + ")");
            } catch (Exception e) {
                throw new PropertyException("Property " + propName + " is missing.", e);
            }
        }
    }

    public void getClassParams(final Class c, final Object obj) {
        getClassParams(null, c, null, obj, Modifier.PUBLIC);
    }

    public void getClassParams(final Logger log, final Class c, final Object obj) {
        getClassParams(log, c, null, obj, Modifier.PUBLIC);
    }

    public void getClassParams(final Logger log, final Class c, final String name, final Object obj) {
        getClassParams(log, c, name, obj, Modifier.PUBLIC);
    }

    public TIntHashSet getIntHashSetFromProperty(final String propertyName) {
        TIntHashSet set = new TIntHashSet();
        for (String id : getProperty(propertyName).split("[, ]")) {
            if (id.length() > 0) {
                set.add(Integer.parseInt(id));
            }
        }
        return set;
    }

    public Set<String> getStringSetFromProperty(final String propertyName) {
        Set<String> set = CollectionUtil.setFromArray(getProperty(propertyName).split("[, ]"));
        set.remove("");
        return set;
    }

    public Set<String> getStringSetFromPropertyWithDefault(final String propertyName, final Set<String> defaultValue) {
        return hasProperty(propertyName) ? getStringSetFromProperty(propertyName) : defaultValue;
    }

    public String[] getStringArrayFromProperty(final String propertyName) {
        return getProperty(propertyName).split("[, ]");
    }

    public String[] getStringArrayFromPropertyWithDefault(final String propertyName, final String[] defaultValue) {
        return hasProperty(propertyName) ? getStringArrayFromProperty(propertyName) : defaultValue;
    }

    public Set<String> getStringSetFromPropertyForClass(final Class c, final String propertyLast) {
        return getStringSetFromProperty(c.getSimpleName() + "." + propertyLast);
    }

    public Set<String> getNameStringSetFromProperty(final String propertyName) {
        final String propertyValue = getProperty(propertyName).trim();
        switch (propertyValue) {
            case "*":
                return CollectionUtil.universalSet();
            case "":
                return Collections.emptySet();
            default:
                Set<String> result = CollectionUtil.setFromArray(propertyValue.split("[, ]+"));
                result.remove("");
                return result;
        }
    }

    public Map<String, String> getNameStringMapFromProperty(final String propertyName) {
        final String propertyValue = getProperty(propertyName).trim();
        if (propertyValue.equals("")) {
            return Collections.emptyMap();
        }
        final Matcher propertyMapMatcher =
                Pattern.compile("\\A\\(\\s*((?:[a-zA-Z0-9\\-\\. ]+=>[a-zA-Z0-9\\-\\. ]+,?)*)\\s*\\)\\Z")
                        .matcher(propertyValue);
        Require.requirement(propertyMapMatcher.matches(), "propertyMapMatcher.matches())");
        Map<String, String> result = new HashMap<>();
        for (String pair : propertyMapMatcher.group(1).split(",")) {
            if (pair.equals("")) {
                continue;
            }
            String[] values = pair.split("=>");
            Require.eq(values.length, "values.length", 2);
            Require.eqNull(result.put(values[0].trim(), values[1].trim()),
                    "result.put(values[0].trim(), values[1].trim())");
        }
        return result;
    }

    public int[] getIntegerArray(final String propertyName) {
        String s = getProperty(propertyName);
        if (s.equals("")) {
            return new int[0];
        }
        String[] parts = s.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i]);
        }
        return result;
    }

    /**
     * Parse a set of non-negative ints from a property. Format is comma-separated individual values and ranges of the
     * form start-end.
     * 
     * @example 0,22,100-200,99,1000-2000
     * @param propertyName
     * @return A set of ints derived from the specified property.
     */
    public TIntSet getNonNegativeIntSetWithRangeSupport(final String propertyName) {
        final String s = getProperty(propertyName);
        final TIntSet result = new gnu.trove.set.hash.TIntHashSet();
        final String tokens[] = s.replaceAll(" +", "").split(",");

        final Pattern rangePattern = Pattern.compile("(\\d+)\\-(\\d+)");
        for (int ti = 0; ti < tokens.length; ++ti) {
            final String token = tokens[ti];
            if (token.length() == 0) {
                continue;
            }
            final Matcher rangeMatcher = rangePattern.matcher(token);
            if (rangeMatcher.matches()) {
                final int rangeBegin = Integer.parseInt(rangeMatcher.group(1));
                final int rangeEnd = Integer.parseInt(rangeMatcher.group(2));
                Assert.assertion(0 <= rangeBegin && rangeBegin <= rangeEnd,
                        "0 <= rangeBegin && rangeBegin <= rangeEnd");
                for (int value = rangeBegin; value <= rangeEnd; ++value) {
                    result.add(value);
                }
            } else {
                final int value = Integer.parseInt(token);
                Assert.assertion(0 <= value, "0 <= value");
                result.add(value);
            }
        }
        return result;
    }

    public BitSet getBitSet(final String propertyName, final int length) {
        String s = getProperty(propertyName);
        if (s.equals("")) {
            return new BitSet(0);
        }
        String[] parts = s.split(",");
        BitSet result = new BitSet(length);
        for (int i = 0; i < parts.length; i++) {
            result.set(Require.inRange(Integer.parseInt(parts[i]), "bit", length, "length"));
        }
        return result;
    }

    public TObjectIntHashMap<String> getStringIntHashMap(final String propertyName, final String separator) {
        final String s = getProperty(propertyName);
        if (s.equals("")) {
            return new TObjectIntHashMap<>();
        }
        final String[] parts = s.split(",");
        final TObjectIntHashMap<String> map = new TObjectIntHashMap<>(parts.length);
        for (int i = 0; i < parts.length; i++) {
            final String[] keyValue = parts[i].split(separator);
            if (keyValue.length != 2) {
                throw new PropertyException("Can't parse property " + propertyName + "=" + s);
            }
            map.put(keyValue[0], Integer.parseInt(keyValue[1]));
        }
        return map;
    }

    public long[] getLongArray(final String propertyName) {
        String s = getProperty(propertyName);
        String[] parts = s.split(",");
        long[] result = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Long.parseLong(parts[i]);
        }
        return result;
    }

}
