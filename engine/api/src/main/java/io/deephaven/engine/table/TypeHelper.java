//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

class TypeHelper {

    // Could be good to move this to io.deephaven.qst.type.Type layer

    public static void checkCastTo(String context, Class<?> srcType, Class<?> destType) {
        if (!destType.isAssignableFrom(srcType)) {
            throw new ClassCastException(String.format("Cannot convert %s of type %s to type %s",
                    context, srcType.getName(), destType.getName()));
        }
    }

    public static void checkCastTo(
            String prefix,
            Class<?> srcType,
            Class<?> srcComponentType,
            Class<?> destType,
            Class<?> destComponentType) {
        checkCastTo(prefix, srcType, destType);
        if ((srcComponentType == null && destComponentType == null) || (srcComponentType != null
                && destComponentType != null && destComponentType.isAssignableFrom(srcComponentType))) {
            return;
        }
        throw new ClassCastException(String.format(
                "Cannot convert %s componentType of type %s to %s (for %s / %s)",
                prefix,
                srcComponentType == null ? null : srcComponentType.getName(),
                destComponentType == null ? null : destComponentType.getName(),
                srcType.getName(),
                destType.getName()));
    }
}
