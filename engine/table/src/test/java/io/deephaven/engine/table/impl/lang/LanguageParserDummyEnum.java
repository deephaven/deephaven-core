//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.lang;

public enum LanguageParserDummyEnum {
    ONE("One"), TWO("Two"), THREE("Red"), FOUR("Blue");

    private final String attribute;

    LanguageParserDummyEnum(String attribute) {
        this.attribute = attribute;
    }

    public String getAttribute() {
        return attribute;
    }
}
