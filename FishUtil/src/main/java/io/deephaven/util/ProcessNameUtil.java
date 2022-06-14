/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

public final class ProcessNameUtil {
    public static String getSuffix() {
        final String fromProp = System.getProperty("processname.suffix");
        if (fromProp == null) {
            return "";
        }
        return "-" + fromProp;
    }

    public static String maybeAddSuffix(final String str) {
        return str + getSuffix();
    }
}
