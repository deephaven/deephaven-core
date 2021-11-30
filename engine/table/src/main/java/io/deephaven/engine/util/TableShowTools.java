/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.time.DateTime;
import io.deephaven.time.TimeZone;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.type.ArrayTypeUtils;

import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The guts of TableTools.show.
 */
class TableShowTools {

    static void showInternal(Table source, long firstRow, long lastRowExclusive, TimeZone timeZone, String delimiter,
            PrintStream out, boolean showRowSet, String[] columns) {
        final QueryPerformanceNugget nugget = QueryPerformanceRecorder.getInstance().getNugget("TableTools.show()");
        try {
            if (columns.length == 0) {
                final List<String> columnNames = source.getDefinition().getColumnNames();
                columns = columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
            }
            final ColumnSource[] columnSources =
                    Arrays.stream(columns).map(source::getColumnSource).toArray(ColumnSource[]::new);

            final RowSet rowSet = source.getRowSet();
            int lineLen = 0;
            final Set<Integer> columnLimits = new HashSet<>();
            if (showRowSet) {
                out.print("RowPosition");
                out.print(delimiter);
                out.print("     RowKey");
                out.print(delimiter);
                columnLimits.add(10);
                columnLimits.add(21);
                lineLen = 22;
            }
            final int[] columnLengths = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (i > 0) {
                    out.print(delimiter);
                    columnLimits.add(lineLen);
                    lineLen++;
                }
                final int columnLen = columnLengths[i] = getColumnLen(column, columnSources[i], rowSet);
                while (columnLen > column.length()) {
                    column = " " + column;
                }
                out.print(column);
                lineLen += column.length();
            }
            out.println();
            for (int i = 0; i < lineLen; i++) {
                if (columnLimits.contains(i)) {
                    if (delimiter.equals("|")) {
                        out.print("+");
                    } else {
                        out.print(delimiter);
                    }
                } else {
                    out.print("-");
                }
            }
            final ColumnPrinter[] columnPrinters = new ColumnPrinter[columns.length];
            for (int i = 0; i < columns.length; i++) {
                columnPrinters[i] = getColumnPrinter(columnSources[i], columnLengths[i], timeZone);
            }

            final ColumnPrinter positionPrinter = new DefaultPrinter(10);
            final ColumnPrinter indexPrinter = new DefaultPrinter(10);
            long ri = 0;
            for (final RowSet.Iterator indexIterator = rowSet.iterator(); ri < lastRowExclusive
                    && indexIterator.hasNext(); ++ri) {
                final long key = indexIterator.nextLong();
                if (ri < firstRow) {
                    continue;
                }
                if (showRowSet) {
                    out.println();
                    positionPrinter.print(out, ri);
                    out.print(delimiter);
                    indexPrinter.print(out, key);
                }
                for (int ci = 0; ci < columns.length; ci++) {
                    if (ci > 0 || showRowSet) {
                        out.print(delimiter);
                    } else {
                        out.println();
                    }
                    columnPrinters[ci].print(out, source.getColumnSource(columns[ci]).get(key));
                }
            }
            out.println();
            out.flush();
        } finally {
            nugget.done();
        }
    }

    private static int getColumnLen(String name, ColumnSource columnSource, RowSet rowSet) {
        int len = name.length();
        if (columnSource.getType().isArray()) {
            len = Math.max(len, 40);
        } else if (columnSource.getType() == long.class || columnSource.getType() == Long.class) {
            len = Math.max(len, 20);
        } else if (columnSource.getType() == double.class || columnSource.getType() == Double.class) {
            len = Math.max(len, 20);
        } else if (columnSource.getType() == DateTime.class) {
            len = Math.max(len, 33);
        } else if (columnSource.getType() == java.util.Date.class) {
            len = Math.max(len, 33);
        } else if (columnSource.getType() == SmartKey.class) {
            len = Math.max(len, 40);
        } else {
            final Annotation annotation = columnSource.getType().getAnnotation(TableToolsShowControl.class);
            if (annotation != null) {
                len = Math.max(len, ((TableToolsShowControl) annotation).getWidth());
            } else {
                len = Math.max(len, 10);
            }
        }
        if (columnSource.getType() == String.class) {
            int ri = 0;
            for (final RowSet.Iterator ii = rowSet.iterator(); ri < 100 && ii.hasNext(); ++ri) {
                String s = (String) columnSource.get(ii.nextLong());
                if (s != null) {
                    len = Math.min(Math.max(s.length(), len), 100);
                }
            }
        }
        return len;
    }

    private static ColumnPrinter getColumnPrinter(ColumnSource column, int len, TimeZone timeZone) {
        if (column.getType() == DateTime.class) {
            return new DateTimePrinter(len, timeZone);
        } else if (!column.getType().isArray()) {
            return new DefaultPrinter(len);
        } else if (!column.getType().getComponentType().isPrimitive()) {
            return new ObjectArrayPrinter(len);
        } else {
            return new PrimitiveArrayPrinter(len);
        }
    }

    private interface ColumnPrinter {
        void print(PrintStream out, Object value);
    }

    private static class DefaultPrinter implements ColumnPrinter {

        private final int len;

        private DefaultPrinter(int len) {
            this.len = len;
        }

        @Override
        public void print(PrintStream out, Object value) {
            String strValue = TableTools.nullToNullString(value);
            boolean isNumber = false;
            if (value instanceof Number) {
                isNumber = true;
            }
            if (strValue.length() > len) {
                boolean isDouble = false;
                if (value instanceof Double) {
                    isDouble = true;
                }
                if (isDouble && strValue.contains("E")) {
                    // sci notation getting chopped
                    String[] chunks = strValue.split("E");
                    int charsOver = strValue.length() - len;
                    int lastCharIndex = chunks[0].length() - charsOver;
                    String partOne = chunks[0].substring(0, lastCharIndex);
                    strValue = partOne + "E" + chunks[1];
                } else if (!isNumber) {
                    strValue = strValue.substring(0, len - 3) + "...";
                }
            } else
                while (strValue.length() < len) {
                    if (isNumber) {
                        strValue = " " + strValue;
                    } else {
                        strValue += " ";
                    }
                }
            out.print(strValue);
        }
    }

    private static abstract class ArrayPrinter implements ColumnPrinter {

        private final int len;

        private ArrayPrinter(int len) {
            this.len = len;
        }

        void printInternal(PrintStream out, Object value) {
            String strValue = "" + value;
            boolean isNumber = false;
            if (value instanceof Number) {
                isNumber = true;
            }
            if (strValue.length() > len) {
                strValue = strValue.substring(0, len - 4) + "...]";
            } else
                while (strValue.length() < len) {
                    if (isNumber) {
                        strValue = " " + strValue;
                    } else {
                        strValue += " ";
                    }
                }
            out.print(strValue);
        }
    }

    private static class ObjectArrayPrinter extends ArrayPrinter {

        private ObjectArrayPrinter(int len) {
            super(len);
        }

        @Override
        public void print(PrintStream out, Object value) {
            printInternal(out, Arrays.toString((Object[]) value));
        }
    }

    private static class DateTimePrinter extends DefaultPrinter {

        private final TimeZone timeZone;

        private DateTimePrinter(int len, TimeZone timeZone) {
            super(len);
            this.timeZone = timeZone;
        }

        @Override
        public void print(PrintStream out, Object value) {
            super.print(out, value == null ? null : ((DateTime) value).toString(timeZone));
        }
    }

    private static class PrimitiveArrayPrinter extends ArrayPrinter {

        private PrimitiveArrayPrinter(int len) {
            super(len);
        }

        @Override
        public void print(PrintStream out, Object value) {
            printInternal(out, Arrays.toString(ArrayTypeUtils.getBoxedArray(value)));
        }
    }
}
