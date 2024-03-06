//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import java.util.Date;
import java.util.Random;

public class DateGenerator extends AbstractSortedGenerator<Date> {
    private final Date minDate;
    private final Date maxDate;

    public DateGenerator(Date minDate, Date maxDate) {
        this.minDate = minDate;
        this.maxDate = maxDate;
    }


    Date maxValue() {
        return maxDate;
    }

    Date minValue() {
        return minDate;
    }

    Date makeValue(Date floor, Date ceiling, Random random) {
        if (floor.equals(ceiling)) {
            return floor;
        }

        if (ceiling.getTime() < floor.getTime()) {
            throw new IllegalStateException("ceiling < floor: " + ceiling + " > " + floor);
        }
        return new Date(floor.getTime() + random.nextInt((int) (ceiling.getTime() - floor.getTime() + 1)));
    }

    @Override
    public Class<Date> getType() {
        return Date.class;
    }
}
