//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.calendar.CalendarInit;
import io.deephaven.time.calendar.StaticCalendarMethods;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static io.deephaven.engine.util.TableTools.emptyTable;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link StaticCalendarMethods} from the {@link Table} API.
 */
@Category(OutOfBandTest.class)
public class TestCalendarMethodsFromTable {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private final Map<Class<?>, Object[]> data = new HashMap<>();
    {
        data.put(String.class, new String[] {"2017-08-01", "2017-08-05"});
        data.put(LocalDate.class, new LocalDate[] {DateTimeUtils.parseLocalDate("2017-08-01"),
                DateTimeUtils.parseLocalDate("2017-08-05")});
        data.put(Instant.class, new Instant[] {DateTimeUtils.parseInstant("2002-01-01T01:00:00.000000000 NY"),
                DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY")});
        data.put(ZonedDateTime.class,
                new ZonedDateTime[] {DateTimeUtils.parseZonedDateTime("2002-01-01T01:00:00.000000000 NY"),
                        DateTimeUtils.parseZonedDateTime("2002-01-21T01:00:00.000000000 NY")});
        data.put(boolean.class, new Boolean[] {true, true});
        data.put(int.class, new Object[] {1, 2});
        data.put(DayOfWeek.class, new Object[] {DayOfWeek.MONDAY, DayOfWeek.TUESDAY});
    }

    private final Map<String, Double> deltas = new HashMap<>();
    {
        deltas.put("X = fractionBusinessDayComplete()", 1e-3);
        deltas.put("X = fractionBusinessDayRemaining()", 1e-3);
    }

    @SuppressWarnings("SameParameterValue")
    private Object getVal(final Table t, final String column) {
        return t.getColumnSource(column).get(t.getRowSet().firstRowKey());
    }

    @SuppressWarnings("StringConcatenationInLoop")
    private void executeTest(final Method m) throws InvocationTargetException, IllegalAccessException {
        final ArrayList<Object> args = new ArrayList<>();
        final Map<Class<?>, Integer> paramCounter = new HashMap<>();

        String query = "X = " + m.getName() + "(";
        boolean isFirst = true;

        for (Class<?> t : m.getParameterTypes()) {
            final int count = paramCounter.getOrDefault(t, 0) + 1;
            paramCounter.put(t, count);

            final String name = t.getSimpleName().toLowerCase() + count;
            final Object[] d = data.get(t);

            if (d == null) {
                throw new RuntimeException("No data for " + t);
            }

            final Object val = d[count - 1];
            QueryScope.addParam(name, val);
            args.add(val);

            if (isFirst) {
                isFirst = false;
            } else {
                query += ", ";
            }

            query += name;
        }

        query += ")";

        System.out.println("Testing " + query);

        final Object target = m.invoke(null, args.toArray());
        final Double delta = deltas.get(query);

        if (delta != null) {
            assertEquals(query, (double) target, (double) getVal(emptyTable(1).update(query), "X"), delta);
        } else if (target instanceof Object[]) {
            // noinspection deprecation
            assertEquals(query, (Object[]) target, (Object[]) getVal(emptyTable(1).update(query), "X"));
        } else {
            assertEquals(query, target, getVal(emptyTable(1).update(query), "X"));
        }
    }

    @Before
    public void setUp() throws Exception {
        CalendarInit.init();
    }

    @Test
    // test to make sure these methods work inside the query strings
    public void testAll() {
        if (!ExecutionContext.getContext().getQueryLibrary().getStaticImports().contains(StaticCalendarMethods.class)) {
            ExecutionContext.getContext().getQueryLibrary().importStatic(StaticCalendarMethods.class);
        }

        for (Method m : StaticCalendarMethods.class.getMethods()) {
            if (m.getDeclaringClass() == Object.class ||
                    !Modifier.isStatic(m.getModifiers()) ||
                    !Modifier.isPublic(m.getModifiers())) {
                continue;
            }

            try {
                executeTest(m);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
