//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.preview;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.util.type.NumericTypeUtils;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyListWrapper;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Converts large data types to Preview types. Also wraps non-serializable data types to be serializable.
 */
public class ColumnPreviewManager {
    // Maps types pt preview factories from the addPreview method
    private static final Map<Class<?>, PreviewColumnFactory<?, ?>> previewMap = new HashMap<>();

    // Factories for arrays and Vectors
    private static final PreviewColumnFactory<Object, ArrayPreview> arrayPreviewFactory =
            new PreviewColumnFactory<>(Object.class, ArrayPreview.class, ArrayPreview::fromArray);
    private static final PreviewColumnFactory<Vector, ArrayPreview> vectorPreviewFactory =
            new PreviewColumnFactory<>(Vector.class, ArrayPreview.class, ArrayPreview::fromVector);
    private static final PreviewColumnFactory<PyListWrapper, ArrayPreview> pyListWrapperPreviewFactory =
            new PreviewColumnFactory<>(PyListWrapper.class, ArrayPreview.class, ArrayPreview::fromPyListWrapper);

    // Factory for non-serializable types
    private static final PreviewColumnFactory<Object, DisplayWrapper> nonDisplayableFactory =
            new PreviewColumnFactory<>(Object.class, DisplayWrapper.class, DisplayWrapper::make);

    private static boolean shouldPreview(Class<?> type) {
        return previewMap.containsKey(type);
    }

    private static final Set<String> whiteList;

    static {
        final String whiteListString =
                Configuration.getInstance().getStringWithDefault("ColumnPreviewManager.whiteListClasses", "");
        whiteList = Arrays.stream(whiteListString.split(",")).map(String::trim).filter(StringUtils::isNotEmpty)
                .collect(Collectors.toSet());
    }

    /**
     * Adds a data type to be converted to a Preview Type.
     *
     * @param sourceType the type to convert
     * @param destType the Preview type to convert to
     * @param function the function that applies the conversion
     * @param <S> the source type
     * @param <D> the destination type
     */
    public static <S, D extends PreviewType> void addPreview(Class<S> sourceType, Class<D> destType,
            Function<S, D> function) {
        previewMap.put(sourceType, new PreviewColumnFactory<>(sourceType, destType, function));
    }

    /**
     * Iterates over a tables columns and applies formulas to allow data to be read in a client UI. Any types not
     * specified in the unpreviewedTypes list will be wrapped in a DisplayWrapper, and if convertArrays is true,
     * arrays/Vectors/PyListWrapper will be converted to ArrayPreview.
     *
     * @param table the table to apply the preview
     * @param convertArrays if true, arrays/Vectors/PyListWrapper will be converted to a string for serialization
     * @param unpreviewedTypes a list of types (by Java canonical name) that should not be previewed or wrapped
     * @return the original table if no columns were changed, otherwise a new table with the preview columns
     */
    public static Table applyPreview(Table table, boolean convertArrays, List<String> unpreviewedTypes) {
        final Map<String, SelectColumn> selectColumns = new HashMap<>();
        final Map<String, ColumnDefinition<?>> columns = table.getDefinition().getColumnNameMap();
        for (String name : columns.keySet()) {
            final ColumnDefinition<?> columnSource = columns.get(name);
            final Class<?> type = columnSource.getDataType();
            String typeName = type.getCanonicalName();
            if (typeName == null) {
                typeName = type.getName();
            }
            if (Vector.class.isAssignableFrom(type)) {
                if (convertArrays) {
                    selectColumns.put(name, vectorPreviewFactory.makeColumn(name));
                }
            } else if (PyListWrapper.class.isAssignableFrom(type)) {
                if (convertArrays) {
                    selectColumns.put(name, pyListWrapperPreviewFactory.makeColumn(name));
                }
            } else if (type.isArray()) {
                if (convertArrays) {
                    selectColumns.put(name, arrayPreviewFactory.makeColumn(name));
                }
            } else if (shouldPreview(type) && !unpreviewedTypes.contains(typeName)) {
                final PreviewColumnFactory<?, ?> factory = previewMap.get(type);
                selectColumns.put(name, factory.makeColumn(name));
            } else if (!unpreviewedTypes.contains(typeName)) {
                // Always wrap non-displayable and non-serializable types
                selectColumns.put(name, nonDisplayableFactory.makeColumn(name));
            }
        }

        if (selectColumns.isEmpty()) {
            return table;
        }
        return makePreviewTable((BaseTable<?>) table, selectColumns);
    }

    /**
     * Iterates over a tables columns and applies a preview (or wraps non-serializable).
     *
     * @param table the table to apply the preview
     * @return the table containing the preview columns
     */
    public static Table applyPreview(final Table table) {
        final Map<String, SelectColumn> selectColumns = new HashMap<>();
        final Map<String, ColumnDefinition<?>> columns = table.getDefinition().getColumnNameMap();
        for (final String name : columns.keySet()) {
            final Class<?> type = columns.get(name).getDataType();
            if (shouldPreview(type)) {
                final PreviewColumnFactory<?, ?> factory = previewMap.get(type);
                selectColumns.put(name, factory.makeColumn(name));
            } else if (Vector.class.isAssignableFrom(type)) {
                // Always wrap Vectors
                selectColumns.put(name, vectorPreviewFactory.makeColumn(name));
            } else if (PyListWrapper.class.isAssignableFrom(type)) {
                // Always wrap PyListWrapper
                selectColumns.put(name, pyListWrapperPreviewFactory.makeColumn(name));
            } else if (type.isArray()) {
                // Always wrap arrays
                selectColumns.put(name, arrayPreviewFactory.makeColumn(name));
            } else if (!isColumnTypeDisplayable(type)) {
                // Always wrap non-displayable and non-serializable types
                selectColumns.put(name, nonDisplayableFactory.makeColumn(name));
            }
        }

        if (selectColumns.isEmpty()) {
            return table;
        }
        return makePreviewTable((BaseTable<?>) table, selectColumns);
    }

    /**
     * Helper to apply the preview columns to a table, and copy relevant attributes, and update the column descriptions
     * to describe changes made.
     *
     * @param table the table to apply the preview columns
     * @param selectColumns the map of column names to SelectColumns
     * @return the new previewed table
     */
    private static @NotNull BaseTable<?> makePreviewTable(BaseTable<?> table, Map<String, SelectColumn> selectColumns) {
        BaseTable<?> result = (BaseTable<?>) table.updateView(selectColumns.values());
        table.copyAttributes(result, BaseTable.CopyAttributeOperation.Preview);
        result.setAttribute(Table.PREVIEW_PARENT_TABLE, table);

        // Add original types to the column descriptions
        final Object attribute = table.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);

        // noinspection unchecked
        final Map<String, String> columnDescriptions =
                attribute != null ? new HashMap<>((Map<String, String>) attribute) : new HashMap<>();

        for (final String name : selectColumns.keySet()) {
            final Class<?> type = table.getDefinition().getColumn(name).getDataType();
            String typeName = type.getCanonicalName();
            if (typeName == null) {
                typeName = type.getName();
            }
            String message = "Preview of type: " + typeName;
            final String currentDescription = columnDescriptions.get(name);
            if (StringUtils.isNotEmpty(currentDescription)) {
                message = message + "\n" + currentDescription;
            }
            columnDescriptions.put(name, message);
        }

        result.setAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE, columnDescriptions);

        return result;
    }

    /**
     * Indicates if a column type is displayable by the client. This is used to screen out unknown classes,
     * unserializable, and anything else that should not be displayed.
     *
     * @param type the column type
     * @return true if the type can be displayed by the client, false otherwise.
     */
    public static boolean isColumnTypeDisplayable(Class<?> type) {
        // Generally arrays and Vectors will be wrapped in an ArrayPreview class. This check is here for correctness.
        if (type.isArray() || Vector.class.isAssignableFrom(type)) {
            // For arrays, we need to check that the component type is displayable
            return isColumnTypeDisplayable(type.getComponentType());
        }

        // These are the allowed types:
        // Primitives
        // Boxed Types
        // String
        // BigInt, BigDecimal
        // Instant/ZonedDateTime/etc
        return type.isPrimitive()
                || io.deephaven.util.type.TypeUtils.isBoxedType(type)
                || io.deephaven.util.type.TypeUtils.isString(type)
                || NumericTypeUtils.isBigNumeric(type)
                || Instant.class == type || ZonedDateTime.class == type
                || LocalDate.class == type || LocalTime.class == type
                || isOnWhiteList(type);
    }

    /**
     * Indicates if a type is on the white list created from user defined properties.
     *
     * @param type the class type
     * @return true if it is on the white list, false otherwise
     */
    public static boolean isOnWhiteList(Class<?> type) {
        return whiteList.contains(type.getName());
    }

    private static class PreviewColumnFactory<S, D> {
        private final Class<S> sourceType;
        private final Class<D> destType;
        private final Function<S, D> function;

        PreviewColumnFactory(Class<S> sourceType, Class<D> destType, Function<S, D> function) {
            this.sourceType = sourceType;
            this.destType = destType;
            this.function = function;
        }

        SelectColumn makeColumn(String name) {
            return new FunctionalColumn<>(name, sourceType, name, destType, function);
        }
    }
}
