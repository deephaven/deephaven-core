package io.deephaven.web.shared.data;

import java.io.Serializable;

/**
 * An opaque representation of a column's value to be used by clients.
 *
 * This allows us to send a column id and a string value, and then translate those into correctly typed objects after
 * serialization.
 *
 * These will be useless without association with a valid table identifier, so the appropriate definition and types can
 * be loaded on the receiving end.
 *
 */
public class ColumnValue implements Serializable {

    public static final char ARRAY_DELIMITER = ',';
    /**
     * Tech debt: we replace all | with \1 in array values when escaping them. actual \1's in values should also be
     * escaped, but will currently fail.
     *
     * Anything transporting arrays of binary data (or weird strings with control characters in them) will need to fixup
     * the ColumnValueRe(/De)hydrater classes which reference this field.
     */
    public static final char ESCAPER = '\1';
    public static final char ESCAPER_ESCAPED = '\2';
    public static final char NULL_SENTINEL = '\0';

    public static String nullSentinel() {
        return Character.toString(NULL_SENTINEL);
    }

    public static String escaper() {
        return Character.toString(ESCAPER);
    }

    public static String arrayDelimiter() {
        return Character.toString(ARRAY_DELIMITER);
    }

    private int columnId;
    private String value;

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "ColumnValue{" +
                "columnId=" + columnId +
                ", value='" + value + '\'' +
                '}';
    }
}
