/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.codegen;

import io.deephaven.engine.util.IterableUtils;
import java.util.Arrays;

public class RichType {
    public static RichType createGeneric(Class type, Class... typeAttributes) {
        return new RichType(type, true, typeAttributes);
    }

    public static RichType createNonGeneric(Class type) {
        return new RichType(type, false, new Class[0]);
    }

    private final Class bareType;
    private final boolean isGeneric;
    private final Class[] typeAttributes;

    private RichType(Class bareType, boolean isGeneric, Class[] typeAttributes) {
        this.bareType = bareType;
        this.isGeneric = isGeneric;
        this.typeAttributes = typeAttributes;
    }

    public Class getBareType() {
        return bareType;
    }

    public boolean isGeneric() {
        return isGeneric;
    }

    public Class[] getTypeAttributes() {
        return typeAttributes;
    }

    public String getCanonicalName() {
        final StringBuilder sb = new StringBuilder();
        sb.append(bareType.getCanonicalName());
        if (isGeneric) {
            sb.append('<');
            sb.append(IterableUtils.makeSeparatedList(Arrays.asList(typeAttributes), ", ", Class::getCanonicalName));
            sb.append('>');
        }
        return sb.toString();
    }
}
