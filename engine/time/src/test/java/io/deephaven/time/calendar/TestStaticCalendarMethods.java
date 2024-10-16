//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * Tests for {@link StaticCalendarMethods}
 * <p>
 * See also {@code StaticCalendarMethodsGenerator}.
 */
public class TestStaticCalendarMethods extends BaseArrayTestCase {
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
        deltas.put("fractionBusinessDayComplete()", 1e-3);
        deltas.put("fractionBusinessDayRemaining()", 1e-3);
    }

    @SuppressWarnings("StringConcatenationInLoop")
    private void executeTest(final Method m1, final Method m2)
            throws InvocationTargetException, IllegalAccessException {
        final ArrayList<Object> args = new ArrayList<>();
        final Map<Class<?>, Integer> paramCounter = new HashMap<>();

        String description = m1.getName() + "(";
        boolean isFirst = true;

        for (Class<?> t : m1.getParameterTypes()) {
            final int count = paramCounter.getOrDefault(t, 0) + 1;
            paramCounter.put(t, count);

            final String name = t.getSimpleName().toLowerCase() + count;
            final Object[] d = data.get(t);

            if (d == null) {
                throw new RuntimeException("No data for " + t);
            }

            final Object val = d[count - 1];
            args.add(val);

            if (isFirst) {
                isFirst = false;
            } else {
                description += ", ";
            }

            description += name;
        }

        description += ")";

        System.out.println("Testing " + description);

        final Object target = m1.invoke(Calendars.calendar(), args.toArray());
        final Object actual = m2.invoke(null, args.toArray());
        final Double delta = deltas.get(description);

        if (delta != null) {
            assertEquals(description, (double) target, (double) actual, delta);
        } else if (target instanceof Object[]) {
            assertEquals(description, (Object[]) target, (Object[]) actual);
        } else {
            assertEquals(description, target, actual);
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        CalendarInit.init();
    }

    // test to make sure these methods work inside the query strings
    public void testAll() {
        final Set<String> excludes = new HashSet<>();
        excludes.add("toString");
        excludes.add("name");
        excludes.add("description");
        excludes.add("firstValidDate");
        excludes.add("lastValidDate");
        excludes.add("clearCache");

        // Occasionally tests fail because of invalid clocks on the test system
        final LocalDate startDate = LocalDate.of(1990, 1, 1);
        final LocalDate currentDate = DateTimeUtils.todayLocalDate();
        assertTrue("Checking for a valid date on the test system: currentDate=" + currentDate,
                currentDate.isAfter(startDate));

        for (Method m1 : BusinessCalendar.class.getMethods()) {
            if (m1.getDeclaringClass() == Object.class ||
                    Modifier.isStatic(m1.getModifiers()) ||
                    !Modifier.isPublic(m1.getModifiers())) {
                continue;
            }

            try {
                String name2 = m1.getName();

                if (excludes.contains(name2)) {
                    System.out.println("Skipping " + name2);
                    continue;
                }

                if (name2.equals("dayOfWeek")) {
                    name2 = "calendarDayOfWeek";
                } else if (name2.equals("dayOfWeekValue")) {
                    name2 = "calendarDayOfWeekValue";
                } else if (name2.equals("timeZone")) {
                    name2 = "calendarTimeZone";
                }

                final Method m2 = StaticCalendarMethods.class.getMethod(name2, m1.getParameterTypes());
                executeTest(m1, m2);
            } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
