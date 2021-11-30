/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.util.tables;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTime;
import io.deephaven.gui.color.Paint;

import java.io.Serializable;
import java.util.Date;

import static io.deephaven.util.QueryConstants.*;

/**
 * Creates {@link ColumnHandler} instances with specified data types.
 */
public class ColumnHandlerFactory implements Serializable {
    private ColumnHandlerFactory() {}

    private static final long serialVersionUID = -6737880834225877984L;

    public enum TypeClassification {
        INTEGER(true), FLOATINGPOINT(true), TIME(true), PAINT(false), COMPARABLE(false), OBJECT(false);

        TypeClassification(boolean isNumeric) {
            this.isNumeric = isNumeric;
        }

        private boolean isNumeric;

        public boolean isNumeric() {
            return isNumeric;
        }
    }

    /**
     * Holds a table column. Allows access to the column's data and the underlying table.
     */
    public interface ColumnHandler {

        /**
         * Gets the handle for the underlying table.
         *
         * @return handle for the table
         */
        TableHandle getTableHandle();

        /**
         * Gets the column's name.
         *
         * @return name of the column being held
         */
        String getColumnName();

        /**
         * Gets the number of rows in the column.
         *
         * @return size of the column
         */
        int size();

        /**
         * Gets the object in row {@code i} of the column.
         *
         * @param i index
         * @return object in row {@code i} of the column
         */
        Object get(final int i);

        /**
         * Gets the data type of the column.
         *
         * @return column's data type
         */
        Class type();

        /**
         * Gets the column's {@link TypeClassification}.
         *
         * @return column's {@link TypeClassification}
         */
        TypeClassification typeClassification();

        /**
         * Gets the object in row {@code i} of the column as a double.
         *
         *
         * @throws UnsupportedOperationException if the value in the column can not be converted to double
         * @param i index
         * @return column's value at row {@code i} as a double
         */
        double getDouble(int i);
    }

    // this version is meant for local use and is not serializable
    private static abstract class ColumnHandlerTable implements ColumnHandler {

        private Table table;
        private final String columnName;
        private final Class type;
        private transient DataColumn dataColumn;
        private final PlotInfo plotInfo;

        private ColumnHandlerTable(final Table table, final String columnName, Class type, final PlotInfo plotInfo) {
            this.type = type;
            ArgumentValidations.assertColumnsInTable(table, plotInfo, columnName);
            this.table = table;
            this.columnName = columnName;
            this.plotInfo = plotInfo;
        }

        protected DataColumn getDataColumn() {
            if (dataColumn == null) {
                dataColumn = table.getColumn(columnName);
            }

            return dataColumn;
        }

        public TableHandle getTableHandle() {
            throw new PlotUnsupportedOperationException("Local ColumnHandler does not support table handles", plotInfo);
        }

        public String getColumnName() {
            return columnName;
        }

        public int size() {
            return getDataColumn().intSize();
        }

        public Object get(final int i) {
            return getDataColumn().get(i);
        }

        public Class type() {
            return type;
        }

        public abstract TypeClassification typeClassification();

        public abstract double getDouble(int i);
    }

    private static abstract class ColumnHandlerHandle implements ColumnHandler, Serializable {

        private static final long serialVersionUID = -3633543948389633718L;
        private final TableHandle tableHandle;
        private final String columnName;
        private final Class type;
        private final PlotInfo plotInfo;
        private transient DataColumn dataColumn;

        private ColumnHandlerHandle(final TableHandle tableHandle, final String columnName, final Class type,
                final PlotInfo plotInfo) {
            this.type = type;
            ArgumentValidations.assertColumnsInTable(tableHandle, plotInfo, columnName);
            this.tableHandle = tableHandle;
            this.columnName = columnName;
            this.plotInfo = plotInfo;
        }

        protected DataColumn getDataColumn() {
            if (dataColumn == null) {
                dataColumn = tableHandle.getTable().getColumn(columnName);
            }

            return dataColumn;
        }

        public TableHandle getTableHandle() {
            return tableHandle;
        }

        public String getColumnName() {
            return columnName;
        }

        public int size() {
            return getDataColumn().intSize();
        }

        public Object get(final int i) {
            return getDataColumn().get(i);
        }

        public Class type() {
            return type;
        }

        public abstract TypeClassification typeClassification();

        public abstract double getDouble(int i);
    }

    /**
     * Creates a new ColumnHandler instance with a numeric {@link TypeClassification}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code tableHandle} and {@code columnName} must not be null.
     * @throws IllegalArgumentException if {@code columnName} is not a column in the table
     * @throws UnsupportedOperationException data in the {@code columnName} must be numeric
     * @param tableHandle holds the table
     * @param columnName column in the table
     * @return new numeric ColumnHandler
     */
    public static ColumnHandler newNumericHandler(final TableHandle tableHandle, final String columnName,
            final PlotInfo plotInfo) {
        ArgumentValidations.assertNotNull(tableHandle, "tableHandle", plotInfo);
        ArgumentValidations.assertNotNull(columnName, "columnName", plotInfo);
        ArgumentValidations.assertColumnsInTable(tableHandle.getTable(), plotInfo, columnName);

        final Class type = tableHandle.getTable().getColumn(columnName).getType();

        if (type.equals(short.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final short value = getDataColumn().getShort(i);
                    return value == NULL_SHORT ? Double.NaN : value;
                }

            };
        } else if (type.equals(int.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final int value = getDataColumn().getInt(i);
                    return value == NULL_INT ? Double.NaN : value;
                }

            };
        } else if (type.equals(long.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final long value = getDataColumn().getLong(i);
                    return value == NULL_LONG ? Double.NaN : value;
                }

            };
        } else if (type.equals(float.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final double value = getDataColumn().getFloat(i);
                    return value == NULL_FLOAT ? Double.NaN : value;
                }

            };
        } else if (type.equals(double.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final double value = getDataColumn().getDouble(i);
                    return value == NULL_DOUBLE ? Double.NaN : value;
                }

            };
        } else if (type.equals(Short.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Integer.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Long.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Float.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Double.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Number.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (Date.class.isAssignableFrom(type)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.TIME;
                }

                @Override
                public double getDouble(int i) {
                    final Date value = (Date) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.getTime() * 1000000;
                }

            };
        } else if (type.equals(DateTime.class)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.TIME;
                }

                @Override
                public double getDouble(int i) {
                    final DateTime value = (DateTime) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.getNanos();
                }

            };
        } else if (Paint.class.isAssignableFrom(type)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.PAINT;
                }

                @Override
                public double getDouble(int i) {
                    throw new UnsupportedOperationException("Double conversion not supported for paints");
                }

            };
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported numeric data type: columnName=" + columnName + " type=" + type);
        }
    }

    /**
     * Creates a new ColumnHandler instance with a numeric {@link TypeClassification}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code columnName} must not be null.
     * @throws IllegalArgumentException if {@code columnName} is not a column in the table
     * @throws UnsupportedOperationException data in the column must be numeric
     * @param table table
     * @param columnName column in the table
     * @return new numeric ColumnHandler
     */
    public static ColumnHandler newNumericHandler(final Table table, final String columnName, final PlotInfo plotInfo) {
        ArgumentValidations.assertNotNull(table, "table", plotInfo);
        ArgumentValidations.assertNotNull(columnName, "columnName", plotInfo);

        final Class type = table.getColumn(columnName).getType();

        if (type.equals(short.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final short value = getDataColumn().getShort(i);
                    return value == NULL_SHORT ? Double.NaN : value;
                }

            };
        } else if (type.equals(int.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final int value = getDataColumn().getInt(i);
                    return value == NULL_INT ? Double.NaN : value;
                }

            };
        } else if (type.equals(long.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final long value = getDataColumn().getLong(i);
                    return value == NULL_LONG ? Double.NaN : value;
                }

            };
        } else if (type.equals(float.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final double value = getDataColumn().getFloat(i);
                    return value == NULL_FLOAT ? Double.NaN : value;
                }

            };
        } else if (type.equals(double.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final double value = getDataColumn().getDouble(i);
                    return value == NULL_DOUBLE ? Double.NaN : value;
                }

            };
        } else if (type.equals(Short.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Integer.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Long.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.INTEGER;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Float.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Double.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Number.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.FLOATINGPOINT;
                }

                @Override
                public double getDouble(int i) {
                    final Number value = (Number) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.doubleValue();
                }

            };
        } else if (type.equals(Date.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.TIME;
                }

                @Override
                public double getDouble(int i) {
                    final Date value = (Date) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.getTime() * 1000000;
                }

            };
        } else if (type.equals(DateTime.class)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.TIME;
                }

                @Override
                public double getDouble(int i) {
                    final DateTime value = (DateTime) getDataColumn().get(i);
                    return value == null ? Double.NaN : value.getNanos();
                }

            };
        } else if (Paint.class.isAssignableFrom(type)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.PAINT;
                }

                @Override
                public double getDouble(int i) {
                    throw new UnsupportedOperationException("Double conversion not supported for paints");
                }

            };
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported numeric data type: columnName=" + columnName + " type=" + type);
        }
    }

    /**
     * Creates a new ColumnHandler instance with a comparable {@link TypeClassification}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code tableHandle} and {@code columnName} must not be null.
     * @throws IllegalArgumentException if {@code columnName} is not a column in the table
     * @throws UnsupportedOperationException data in the {@code columnName} must be {@link Comparable}
     * @param tableHandle holds the table
     * @param columnName column in the table
     * @return new comparable ColumnHandler
     */
    @SuppressWarnings("WeakerAccess")
    public static ColumnHandler newComparableHandler(final TableHandle tableHandle, final String columnName,
            final PlotInfo plotInfo) {
        ArgumentValidations.assertNotNull(tableHandle, "tableHandle", plotInfo);
        ArgumentValidations.assertNotNull(columnName, "columnName", plotInfo);

        final Class type = tableHandle.getTable().getColumn(columnName).getType();

        if (Comparable.class.isAssignableFrom(type)) {
            return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.COMPARABLE;
                }

                @Override
                public double getDouble(int i) {
                    throw new PlotUnsupportedOperationException("Double conversion not supported for comparables",
                            plotInfo);
                }

            };
        } else {
            throw new PlotUnsupportedOperationException(
                    "Unsupported data type: columnName=" + columnName + " type=" + type, plotInfo);
        }
    }

    /**
     * Creates a new ColumnHandler instance with a comparable {@link TypeClassification}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code columnName} must not be null.
     * @throws IllegalArgumentException if {@code columnName} is not a column in the {@code table}
     * @throws UnsupportedOperationException data in the column must be {@link Comparable}
     * @param table table
     * @param columnName column in the table
     * @return new comparable ColumnHandler
     */
    @SuppressWarnings("WeakerAccess")
    public static ColumnHandler newComparableHandler(final Table table, final String columnName,
            final PlotInfo plotInfo) {
        ArgumentValidations.assertNotNull(table, "table", plotInfo);
        ArgumentValidations.assertNotNull(columnName, "columnName", plotInfo);

        final Class type = table.getColumn(columnName).getType();

        if (Comparable.class.isAssignableFrom(type)) {
            return new ColumnHandlerTable(table, columnName, type, plotInfo) {
                @Override
                public TypeClassification typeClassification() {
                    return TypeClassification.COMPARABLE;
                }

                @Override
                public double getDouble(int i) {
                    throw new PlotUnsupportedOperationException("Double conversion not supported for comparables",
                            plotInfo);
                }

            };
        } else {
            throw new PlotUnsupportedOperationException(
                    "Unsupported data type: columnName=" + columnName + " type=" + type, plotInfo);
        }
    }

    /**
     * Creates a new ColumnHandler instance with a object {@link TypeClassification}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code tableHandle} and {@code columnName} must not be null.
     * @throws IllegalArgumentException if {@code columnName} is not a column in the table
     * @param tableHandle holds the table
     * @param columnName column in the table
     * @return new object ColumnHandler
     */
    public static ColumnHandler newObjectHandler(final TableHandle tableHandle, final String columnName,
            final PlotInfo plotInfo) {
        ArgumentValidations.assertNotNull(tableHandle, "tableHandle", plotInfo);
        ArgumentValidations.assertNotNull(columnName, "columnName", plotInfo);

        final Class type = tableHandle.getTable().getColumn(columnName).getType();
        return new ColumnHandlerHandle(tableHandle, columnName, type, plotInfo) {
            @Override
            public TypeClassification typeClassification() {
                return TypeClassification.OBJECT;
            }

            @Override
            public double getDouble(int i) {
                throw new PlotUnsupportedOperationException("Double conversion not supported for objects", plotInfo);
            }
        };
    }

    /**
     * Creates a new ColumnHandler instance with a object {@link TypeClassification}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code columnName} must not be null.
     * @throws IllegalArgumentException if {@code columnName} is not a column in the {@code table}
     * @param table table
     * @param columnName column in the table
     * @return new object ColumnHandler
     * @throws io.deephaven.base.verify.RequirementFailure {@code table} and {@code columnName} must not be null.
     * @throws IllegalArgumentException if {@code columnName} is not a column in the {@code table}
     */
    public static ColumnHandler newObjectHandler(final Table table, final String columnName, final PlotInfo plotInfo) {
        ArgumentValidations.assertNotNull(table, "table", plotInfo);
        ArgumentValidations.assertNotNull(columnName, "columnName", plotInfo);

        final Class type = table.getColumn(columnName).getType();
        return new ColumnHandlerTable(table, columnName, type, plotInfo) {
            @Override
            public TypeClassification typeClassification() {
                return TypeClassification.OBJECT;
            }

            @Override
            public double getDouble(int i) {
                throw new PlotUnsupportedOperationException("Double conversion not supported for objects", plotInfo);
            }
        };
    }
}
