/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.preview;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.Table;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.jpy.PyListWrapper;

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
     * Iterates over a tables columns and applies a preview (or wraps non-serializable).
     *
     * @param table the table to apply the preview
     * @return the table containing the preview columns
     */
    public static Table applyPreview(final Table table) {
        BaseTable<?> result = (BaseTable<?>) table;
        final List<SelectColumn> selectColumns = new ArrayList<>();
        final Map<String, ? extends ColumnSource<?>> columns = table.getColumnSourceMap();
        final Map<String, String> originalTypes = new HashMap<>();
        for (String name : columns.keySet()) {
            final ColumnSource<?> columnSource = columns.get(name);
            final Class<?> type = columnSource.getType();
            String typeName = type.getCanonicalName();
            if (typeName == null) {
                typeName = type.getName();
            }
            if (shouldPreview(type)) {
                final PreviewColumnFactory<?, ?> factory = previewMap.get(type);
                selectColumns.add(factory.makeColumn(name));
                originalTypes.put(name, typeName);
            } else if (Vector.class.isAssignableFrom(type)) {
                // Always wrap Vectors
                selectColumns.add(vectorPreviewFactory.makeColumn(name));
                originalTypes.put(name, typeName);
            } else if (PyListWrapper.class.isAssignableFrom(type)) {
                // Always wrap PyListWrapper
                selectColumns.add(pyListWrapperPreviewFactory.makeColumn(name));
                originalTypes.put(name, typeName);
            } else if (type.isArray()) {
                // Always wrap arrays
                selectColumns.add(arrayPreviewFactory.makeColumn(name));
                originalTypes.put(name, typeName);
            } else if (!isColumnTypeDisplayable(type)
                    || !io.deephaven.util.type.TypeUtils.isPrimitiveOrSerializable(type)) {
                // Always wrap non-displayable and non-serializable types
                selectColumns.add(nonDisplayableFactory.makeColumn(name));
                originalTypes.put(name, typeName);
            }
        }

        if (!selectColumns.isEmpty()) {
            result = (BaseTable<?>) table.updateView(selectColumns);
            ((BaseTable<?>) table).copyAttributes(result, BaseTable.CopyAttributeOperation.Preview);
            result.setAttribute(Table.PREVIEW_PARENT_TABLE, table);

            // Add original types to the column descriptions
            final Object attribute = table.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);

            // noinspection unchecked
            final Map<String, String> columnDescriptions =
                    attribute != null ? new HashMap<>((Map<String, String>) attribute) : new HashMap<>();

            for (String name : originalTypes.keySet()) {
                String message = "Preview of type: " + originalTypes.get(name);
                final String currentDescription = columnDescriptions.get(name);
                if (StringUtils.isNotEmpty(currentDescription)) {
                    message = message + "\n" + currentDescription;
                }
                columnDescriptions.put(name, message);
            }

            result.setAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE, columnDescriptions);
        }

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
        // DateTime
        return type.isPrimitive() || io.deephaven.util.type.TypeUtils.isBoxedType(type)
                || io.deephaven.util.type.TypeUtils.isString(type)
                || io.deephaven.util.type.TypeUtils.isBigNumeric(type) || TypeUtils.isDateTime(type)
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
