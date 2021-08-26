package io.deephaven.db.tables.lang;

public enum DBLanguageParserDummyEnum {
    ONE("One"), TWO("Two"), THREE("Red"), FOUR("Blue");

    private final String attribute;

    DBLanguageParserDummyEnum(String attribute) {
        this.attribute = attribute;
    }

    public String getAttribute() {
        return attribute;
    }
}
