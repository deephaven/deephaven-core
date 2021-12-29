package io.deephaven.engine.util;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.type.EnumValue;
import org.jetbrains.annotations.NotNull;

import javax.swing.table.TableCellRenderer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.Table.COLUMN_RENDERERS_ATTRIBUTE;

/**
 * Builds and parses the directive for Table.COLUMN_RENDERERS_ATTRIBUTE.
 */
@ScriptApi
public class ColumnRenderersBuilder {

    public enum ColumnRendererType {
        /**
         * Indicates that the default renderer should be used for the column.
         */
        DEFAULT,

        /**
         * Indicates that progress bar renderer should be used for the column.
         */
        PROGRESS_BAR
    }

    private final Map<String, String> columnToRendererMap = new HashMap<>();
    private final Map<ColumnRendererType, String> typeToClassMap = new HashMap<>();

    /**
     * Sets the default class name to use for the default render type.
     *
     * @param className the fully qualified class name of the column renderer
     */
    public ColumnRenderersBuilder setDefaultRenderClass(String className) {
        return setRenderClass(ColumnRendererType.DEFAULT, className);
    }

    /**
     * Sets the class name associated with a given render type.
     *
     * @param renderType the render type to set
     * @param className the fully qualified class name of the column renderer
     */
    public ColumnRenderersBuilder setRenderClass(ColumnRendererType renderType, String className) {
        typeToClassMap.put(renderType, className);
        return this;
    }

    @ScriptApi
    public ColumnRenderersBuilder setRenderer(String columnName, String rendererName) {
        columnToRendererMap.put(columnName, rendererName);
        return this;
    }

    /**
     * Sets a column renderer for a column of a given name.
     *
     * @param columnName the name of the column
     * @param rendererType the type of renderer
     */
    @ScriptApi
    public ColumnRenderersBuilder setRenderer(String columnName, ColumnRendererType rendererType) {
        return setRenderer(columnName, rendererType.name());
    }

    @ScriptApi
    public ColumnRenderersBuilder setRenderer(String columnName, Class<? extends TableCellRenderer> rendererClass) {
        return setRenderer(columnName, rendererClass.getCanonicalName());
    }

    /**
     * Removes the renderer for a column of a given name.
     *
     * @param columnName the name of the column
     */
    @ScriptApi
    public ColumnRenderersBuilder removeRenderer(String columnName) {
        columnToRendererMap.remove(columnName);
        return this;
    }

    /**
     * Clears all column renderers.
     */
    @ScriptApi
    public ColumnRenderersBuilder clear() {
        columnToRendererMap.clear();
        return this;
    }

    /**
     * Indicates if a specific column has a renderer set in the builder.
     *
     * @param columnName the name of the column to check
     * @return true if it is set, false otherwise
     */
    public boolean isColumnRendererSet(String columnName) {
        return columnToRendererMap.containsKey(columnName);
    }

    /**
     * Gets the fully qualified class name for the renderer of a column.
     *
     * @param columnName the column name
     * @return the fully qualified class name of the renderer
     */
    public String getRenderClassName(String columnName) {
        ColumnRendererType type = getRendererType(columnName);

        if (type == null) {
            return columnToRendererMap.get(columnName);
        }

        return getRenderClassForType(type);
    }

    /**
     * Gets the column renderer type assigned to a given column name. Returns null if none is assigned.
     *
     * @param columnName the name of the column
     * @return the renderer type, null if none is assigned
     */
    public ColumnRendererType getRendererType(String columnName) {
        String text = columnToRendererMap.get(columnName);

        try {
            return EnumValue.caseInsensitiveValueOf(ColumnRendererType.class, text);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public String getRenderClassForType(ColumnRendererType type) {
        return typeToClassMap.get(type);
    }

    /**
     * Identifies if this builder-instance contains any renderer-definitions. If not, then the calling method should not
     * attempt to add our directive an an attribute to a table
     *
     * @return true if there are no renderers defined, else false
     */
    public boolean isEmpty() {
        return columnToRendererMap.isEmpty();
    }

    /**
     * Returns a Set of column-names, which may be verified as valid prior to setting our directive as an attribute to
     * the table
     *
     * @return An iterable Set of column-names identified by this builder-instance
     */
    public Set<String> getColumnSet() {
        return columnToRendererMap.keySet();
    }

    /**
     * Gets a ColumnRenderersBuilder from the COLUMN_RENDERERS_ATTRIBUTE in a source Table.
     *
     * @param source the Table
     * @return the ColumnRenderersBuilder from the Table
     */
    public static ColumnRenderersBuilder get(Table source) {
        return fromDirective((String) source.getAttribute(COLUMN_RENDERERS_ATTRIBUTE));
    }

    /**
     * Creates a new ColumnRenderersBuilder from an existing COLUMN_RENDERERS_ATTRIBUTE directive.
     *
     * @param directive a valid COLUMN_RENDERERS_ATTRIBUTE directive
     * @return a ColumnRenderersBuilder represented by the directive
     */
    public static ColumnRenderersBuilder fromDirective(final String directive) {
        final ColumnRenderersBuilder builder = new ColumnRenderersBuilder();
        if (directive == null || directive.isEmpty()) {
            return builder;
        }

        final String[] pairs = directive.split(",");
        for (final String pair : pairs) {
            if (pair.trim().isEmpty())
                continue;
            final String[] kv = pair.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid " + COLUMN_RENDERERS_ATTRIBUTE + ": " + directive
                        + ", bad column renderer pair " + pair);
            }
            builder.setRenderer(kv[0], kv[1]);
        }

        return builder;
    }

    public static ColumnRenderersBuilder empty() {
        return new ColumnRenderersBuilder();
    }

    /**
     * Builds the COLUMN_RENDERERS_ATTRIBUTE directive from the data in this ColumnRenderersBuilder.
     *
     * @return a String representing the COLUMN_RENDERERS_ATTRIBUTE directive
     */
    public String buildDirective() {
        final StringBuilder builder = new StringBuilder();
        columnToRendererMap.forEach((k, v) -> builder.append(k).append("=").append(v).append(","));
        return builder.toString();
    }

    /**
     * Helper method for validating, building, and {@link Table#setColumnRenderers(String) applying} column renderers to
     * a {@link Table}.
     *
     * @param table The source {@link Table}
     * @return {@code table.setColumnRenderers(buildDirective())}, or {@code table} if {@link #isEmpty()}
     */
    @ScriptApi
    public Table applyToTable(@NotNull final Table table) {
        if (isEmpty()) {
            if (table.isRefreshing()) {
                LivenessScopeStack.peek().manage(table);
            }
            return table;
        }

        final Set<String> existingColumns = table.getDefinition().getColumnNames()
                .stream()
                .filter(column -> !ColumnFormattingValues.isFormattingColumn(column))
                .collect(Collectors.toSet());

        final String[] unknownColumns = getColumnSet()
                .stream()
                .filter(column -> !existingColumns.contains(column))
                .toArray(String[]::new);

        if (unknownColumns.length > 0) {
            throw new RuntimeException(
                    "Unknown columns: " + Arrays.toString(unknownColumns) + ", available columns = " + existingColumns);
        }

        return table.setColumnRenderers(buildDirective());
    }

    @Override
    public String toString() {
        return "{ColumnRenderersBuilder: " + buildDirective() + "}";
    }
}
