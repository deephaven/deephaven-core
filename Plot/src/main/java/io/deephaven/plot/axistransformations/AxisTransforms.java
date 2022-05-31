/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.axistransformations;

import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Common {@link AxisTransform}s.
 */
public enum AxisTransforms implements AxisTransform, Serializable {
    /**
     * Natural logarithm. Non-positive data values are not displayed.
     */
    LOG((DoubleUnaryOperator & Serializable) x -> x <= 0.0 ? Double.NaN : Math.log(x),
            (DoubleUnaryOperator & Serializable) Math::exp, (DoublePredicate & Serializable) x -> x > 0.0),

    /**
     * Square root. Negative data values are not displayed.
     */
    SQRT((DoubleUnaryOperator & Serializable) x -> x < 0.0 ? Double.NaN : Math.sqrt(x),
            (DoubleUnaryOperator & Serializable) x -> x * x, (DoublePredicate & Serializable) x -> x >= 0.0);

    private final DoubleUnaryOperator transform;
    private final DoubleUnaryOperator inverseTransform;
    private final DoublePredicate isVisible;

    AxisTransforms(final DoubleUnaryOperator dataToAxis, final DoubleUnaryOperator axisToData,
            final DoublePredicate isVisible) {
        this.transform = dataToAxis;
        this.inverseTransform = axisToData;
        this.isVisible = isVisible;
    }


    @Override
    public double transform(final double dataValue) {
        return transform.applyAsDouble(dataValue);
    }

    @Override
    public double inverseTransform(final double axisValue) {
        return inverseTransform.applyAsDouble(axisValue);
    }

    @Override
    public boolean isVisible(final double dataValue) {
        return isVisible.test(dataValue);
    }

    private static final Logger log = Logger.getLogger(AxisTransforms.class.toString());

    /**
     * Returns an axis transform.
     *
     * @param name case insensitive transform name.
     * @throws IllegalArgumentException {@code name} must not be null.
     * @return requested axis transform.
     */
    @SuppressWarnings("ConstantConditions")
    public static AxisTransform axisTransform(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("AxisTransform can not be null!");
        }

        AxisTransform at1, at2;

        try {
            at1 = valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            at1 = null;
        }

        try {
            final BusinessCalendar cal = Calendars.calendar(name);
            at2 = new AxisTransformBusinessCalendar(cal);
        } catch (IllegalArgumentException e) {
            at2 = null;
        }

        if (at1 != null && at2 != null) {
            log.warning(
                    "Axis transform is defined in both enum and calendar.  Returning the enum value.  name=" + name);
            return at1;
        } else if (at1 != null) {
            return at1;
        } else if (at2 != null) {
            return at2;
        } else {
            throw new IllegalArgumentException("AxisTransform " + name + " is not defined");
        }
    }

    /**
     * Returns the names of available axis transforms.
     *
     * @return an array of the available axis transform names.
     */
    public static String[] axisTransformNames() {
        final Set<String> results =
                Arrays.stream(values()).map(Enum::name).collect(Collectors.toCollection(LinkedHashSet::new));
        final Set<String> calendars = new LinkedHashSet<>(Arrays.asList(Calendars.calendarNames()));
        final Set<String> conflicts = new LinkedHashSet<>(results);
        final boolean hasConflicts = conflicts.retainAll(calendars);

        if (hasConflicts) {
            log.warning("AxisTransform enum and calendar names have conflicting values: values=" + conflicts);
        }

        results.addAll(calendars);
        return results.toArray(new String[0]);
    }


}
