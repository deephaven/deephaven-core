//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.api.filter.FilterValue;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;

import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.IntStream.Builder;

/**
 * Describes the structure of the column, and if desired can be used to get access to the data to be rendered in this
 * column.
 */
@TsName(namespace = "dh")
public class Column {
    private final int index;

    private final Integer styleColumnIndex;
    private final Integer formatStringColumnIndex;

    private final boolean isPartitionColumn;

    private final String type;

    private final String name;

    private final boolean isSortable;

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

    /**
     * Format entire rows colors using the expression specified. Returns a <b>CustomColumn</b> object to apply to a
     * table using <b>applyCustomColumns</b> with the parameters specified.
     *
     * @param expression
     * @return {@link CustomColumn}
     */
    @JsMethod(namespace = "dh.Column")
    public static CustomColumn formatRowColor(String expression) {
        return new CustomColumn(CustomColumn.ROW_FORMAT_NAME, CustomColumn.TYPE_FORMAT_COLOR, expression);
    }

    /**
     * a <b>CustomColumn</b> object to apply using <b>applyCustomColumns</b> with the expression specified.
     *
     * @param name
     * @param expression
     * @return {@link CustomColumn}
     */
    @JsMethod(namespace = "dh.Column")
    public static CustomColumn createCustomColumn(String name, String expression) {
        return new CustomColumn(name, CustomColumn.TYPE_NEW, expression);
    }

    public Column(int jsIndex, int index, Integer formatColumnIndex, Integer styleColumnIndex, String type, String name,
            boolean isPartitionColumn, Integer formatStringColumnIndex, String description,
            boolean inputTableKeyColumn, boolean isSortable) {
        this.jsIndex = jsIndex;
        this.index = index;
        assert Objects.equals(formatColumnIndex, styleColumnIndex);
        this.styleColumnIndex = styleColumnIndex;
        this.type = type;
        this.name = name;
        this.isPartitionColumn = isPartitionColumn;
        this.formatStringColumnIndex = formatStringColumnIndex;
        this.description = description;
        this.isInputTableKeyColumn = inputTableKeyColumn;
        this.isSortable = isSortable;
    }

    /**
     * the value for this column in the given row. Type will be consistent with the type of the Column.
     *
     * @param row
     * @return Any
     */
    @JsMethod
    public Any get(TableData.Row row) {
        return row.get(this);
    }

    @JsMethod
    public Format getFormat(TableData.Row row) {
        return row.getFormat(this);
    }

    /**
     * @deprecated do not use. Internal index of the column in the table, to be used as a key on the Row.
     * @return int
     */
    @Deprecated
    @JsProperty(name = "index")
    public int getJsIndex() {
        return jsIndex;
    }

    public int getIndex() {
        return index;
    }

    /**
     * Type of the row data that can be found in this column.
     *
     * @return String
     */
    @JsProperty
    public String getType() {
        return type;
    }

    /**
     * Label for this column.
     *
     * @return String
     */
    @JsProperty
    public String getName() {
        return name;
    }

    @JsProperty
    @JsNullable
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

    /**
     * If this column is part of a roll-up tree table, represents the type of the row data that can be found in this
     * column for leaf nodes if includeConstituents is enabled. Otherwise, it is <b>null</b>.
     *
     * @return String
     */
    @JsProperty
    @JsNullable
    public String getConstituentType() {
        return constituentType;
    }

    public void setConstituentType(final String constituentType) {
        this.constituentType = constituentType;
    }

    public Integer getFormatStringColumnIndex() {
        return formatStringColumnIndex;
    }

    public Integer getStyleColumnIndex() {
        return styleColumnIndex;
    }

    /**
     * True if this column is a partition column. Partition columns are used for filtering uncoalesced tables - see
     * {@link JsTable#isUncoalesced()}.
     *
     * @return true if the column is a partition column
     */
    @JsProperty
    public boolean getIsPartitionColumn() {
        return isPartitionColumn;
    }

    public boolean isInputTableKeyColumn() {
        return isInputTableKeyColumn;
    }

    /**
     * Creates a sort builder object, to be used when sorting by this column.
     *
     * @return {@link Sort}
     */
    @JsMethod
    public Sort sort() {
        return new Sort(this);
    }

    @JsProperty
    public boolean getIsSortable() {
        return isSortable;
    }

    /**
     * Creates a new value for use in filters based on this column. Used either as a parameter to another filter
     * operation, or as a builder to create a filter operation.
     *
     * @return {@link FilterValue}
     */
    @JsMethod
    public FilterValue filter() {
        return new FilterValue(this);
    }

    /**
     * a <b>CustomColumn</b> object to apply using `applyCustomColumns` with the expression specified.
     *
     * @param expression
     * @return {@link CustomColumn}
     */
    @JsMethod
    public CustomColumn formatColor(String expression) {
        return new CustomColumn(name, CustomColumn.TYPE_FORMAT_COLOR, expression);
    }

    /**
     * a <b>CustomColumn</b> object to apply using <b>applyCustomColumns</b> with the expression specified.
     *
     * @param expression
     * @return {@link CustomColumn}
     */
    @JsMethod
    public CustomColumn formatNumber(String expression) {
        return new CustomColumn(name, CustomColumn.TYPE_FORMAT_NUMBER, expression);
    }

    /**
     * a <b>CustomColumn</b> object to apply using <b>applyCustomColumns</b> with the expression specified.
     *
     * @param expression
     * @return {@link CustomColumn}
     */
    @JsMethod
    public CustomColumn formatDate(String expression) {
        return new CustomColumn(name, CustomColumn.TYPE_FORMAT_DATE, expression);
    }

    @JsMethod
    @Override
    public String toString() {
        return "Column{" +
                "index=" + index +
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
        result = 31 * result + (styleColumnIndex != null ? styleColumnIndex.hashCode() : 0);
        result = 31 * result + (formatStringColumnIndex != null ? formatStringColumnIndex.hashCode() : 0);
        result = 31 * result + type.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    public Column withFormatStringColumnIndex(int formatStringColumnIndex) {
        return new Column(jsIndex, index, styleColumnIndex, styleColumnIndex, type, name, isPartitionColumn,
                formatStringColumnIndex, description, isInputTableKeyColumn, isSortable);
    }

    public Column withStyleColumnIndex(int styleColumnIndex) {
        return new Column(jsIndex, index, styleColumnIndex, styleColumnIndex, type, name, isPartitionColumn,
                formatStringColumnIndex, description, isInputTableKeyColumn, isSortable);
    }
}
