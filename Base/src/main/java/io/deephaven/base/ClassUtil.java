/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public final class ClassUtil {
    public static String getBaseName(final String s) {
        int i = s.lastIndexOf(".");
        return (i == -1) ? s : s.substring(i + 1);
    }

    public static void dumpFinals(final Logger log, final String prefix, final Object p) {
        final Class c = p.getClass();
        Field[] fields = c.getDeclaredFields();
        final int desiredMods = (Modifier.PUBLIC|Modifier.FINAL);
        for (Field f : fields) {
            if ( (f.getModifiers() & desiredMods) == 0 ) continue;
            try {
                final String tName = f.getType().getName();
                final String name = f.getName();
                final Object value = f.get(p);
                log.info(  prefix
                         + tName
                         + " " + name
                         + " = " + value.toString());
            }
            catch (Exception ignored) {}
        }
    }

    public static <T> Class<T> generify(Class c) {
        return c;
    }
}
