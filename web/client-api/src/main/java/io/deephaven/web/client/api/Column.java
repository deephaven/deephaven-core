package io.deephaven.web.client.api;

import io.deephaven.web.client.api.filter.FilterValue;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;

import java.util.stream.IntStream;
import java.util.stream.IntStream.Builder;

public class Column {
    private final int index;

    private final Integer formatColumnIndex;
    private final Integer styleColumnIndex;
    private final Integer formatStringColumnIndex;

    private final boolean isPartitionColumn;

    private final String type;

    private final String name;

    @Deprecated
    private final int jsIndex;

    /**
     * Specific to rollup tables when constituent columns are enabled. Used in toString(), but ignored for
     * equals/hashcode, since it might be helpful for debugging, but could potentially confuse some comparisons between
     * instances since this is set after the CTS is created, ready for use.
     */
    private String constituentType;

    private String description;
    private final boolean isInputTableKeyColumn;

    @JsMethod(namespace = "dh.Column")
    public static CustomColumn formatRowColor(String expression) {
        return new CustomColumn(CustomColumn.ROW_FORMAT_NAME, CustomColumn.TYPE_FORMAT_COLOR, expression);
    }

    @JsMethod(namespace = "dh.Column")
    public static CustomColumn createCustomColumn(String name, String expression) {
        return new CustomColumn(name, CustomColumn.TYPE_NEW, expression);
    }

    public Column(int jsIndex, int index, Integer formatColumnIndex, Integer styleColumnIndex, String type, String name,
            boolean isPartitionColumn, Integer formatStringColumnIndex, String description,
            boolean inputTableKeyColumn) {
        this.jsIndex = jsIndex;
        this.index = index;
        this.formatColumnIndex = formatColumnIndex;
        this.styleColumnIndex = styleColumnIndex;
        this.type = type;
        this.name = name;
        this.isPartitionColumn = isPartitionColumn;
        this.formatStringColumnIndex = formatStringColumnIndex;
        this.description = description;
        this.isInputTableKeyColumn = inputTableKeyColumn;
    }

    @JsMethod
    public Any get(TableData.Row row) {
        return row.get(this);
    }

    @JsMethod
    public Format getFormat(TableData.Row row) {
        return row.getFormat(this);
    }

    @Deprecated
    @JsProperty(name = "index")
    public int getJsIndex() {
        return jsIndex;
    }

    public int getIndex() {
        return index;
    }

    @JsProperty
    public String getType() {
        return type;
    }

    @JsProperty
    public String getName() {
        return name;
    }

    @JsProperty
    public String getDescription() {
        return description;
    }

    public IntStream getRequiredColumns() {
        Builder builder = IntStream.builder();
        builder.accept(index);
        if (formatStringColumnIndex != null) {
            builder.accept(formatStringColumnIndex);
        }
        if (styleColumnIndex != null) {
            builder.accept(styleColumnIndex);
        }
        return builder.build();
    }

    @JsProperty
    public String getConstituentType() {
        return constituentType;
    }

    public void setConstituentType(final String constituentType) {
        this.constituentType = constituentType;
    }

    /**
     * @deprecated Prefer {@link #getFormatStringColumnIndex()}.
     */
    @Deprecated
    public Integer getFormatColumnIndex() {
        return formatColumnIndex;
    }

    public Integer getFormatStringColumnIndex() {
        return formatStringColumnIndex;
    }

    public Integer getStyleColumnIndex() {
        return styleColumnIndex;
    }

    @JsProperty
    public boolean getIsPartitionColumn() {
        return isPartitionColumn;
    }

    public boolean isInputTableKeyColumn() {
        return isInputTableKeyColumn;
    }

    @JsMethod
    public Sort sort() {
        return new Sort(this);
    }

    @JsMethod
    public FilterValue filter() {
        return new FilterValue(this);
    }

    @JsMethod
    public CustomColumn formatColor(String expression) {
        return new CustomColumn(name, CustomColumn.TYPE_FORMAT_COLOR, expression);
    }

    @JsMethod
    public CustomColumn formatNumber(String expression) {
        return new CustomColumn(name, CustomColumn.TYPE_FORMAT_NUMBER, expression);
    }

    @JsMethod
    public CustomColumn formatDate(String expression) {
        return new CustomColumn(name, CustomColumn.TYPE_FORMAT_DATE, expression);
    }

    @JsMethod
    @Override
    public String toString() {
        return "Column{" +
                "index=" + index +
                ", formatColumnIndex=" + formatColumnIndex +
                ", styleColumnIndex=" + styleColumnIndex +
                ", formatStringColumnIndex=" + formatStringColumnIndex +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Column column = (Column) o;

        if (index != column.index)
            return false;
        if (formatColumnIndex != null ? !formatColumnIndex.equals(column.formatColumnIndex)
                : column.formatColumnIndex != null)
            return false;
        if (styleColumnIndex != null ? !styleColumnIndex.equals(column.styleColumnIndex)
                : column.styleColumnIndex != null)
            return false;
        if (formatStringColumnIndex != null ? !formatStringColumnIndex.equals(column.formatStringColumnIndex)
                : column.formatStringColumnIndex != null)
            return false;
        if (!type.equals(column.type))
            return false;
        return name.equals(column.name);
    }

    @Override
    public int hashCode() {
        int result = index;
        result = 31 * result + (formatColumnIndex != null ? formatColumnIndex.hashCode() : 0);
        result = 31 * result + (styleColumnIndex != null ? styleColumnIndex.hashCode() : 0);
        result = 31 * result + (formatStringColumnIndex != null ? formatStringColumnIndex.hashCode() : 0);
        result = 31 * result + type.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
