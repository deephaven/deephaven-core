/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * A time period that can be expressed in an easily human-readable format.
 */
public class Period implements Comparable<Period>, Serializable {
    //TODO: test coverage
    //TODO: replace fully with java.time.Period?

    private String periodString;

    //TODO: replaced by java.time.Period
    //TODO: see https://stackoverflow.com/questions/47217098/converting-org-joda-time-period-to-java-time-period
    //TODO: https://docs.oracle.com/javase/8/docs/api/java/time/Period.html#parse-java.lang.CharSequence-
    private transient org.joda.time.Period period = null;

    /**
     * Constructor for serialization / deserialization.  This should not be used.
     */
    public Period() {
        // for serialization...
    }

    /**
     * Create a new Period initialized to the provided period string.
     *
     * @param s string in the form of "nYnMnWnDTnHnMnS", with n being numeric values, e.g. 1W for one week, T1M for
     *          one minute, 1WT1H for one week plus one hour.
     */
    public Period(String s) {
        char[] ret = s.toCharArray();

        for (int i = 0; i < ret.length; i++) {
            if (Character.toUpperCase(ret[i]) != 'T') {
                ret[i] = Character.toLowerCase(ret[i]);
            } else {
                ret[i] = Character.toUpperCase(ret[i]);
            }
        }

        this.periodString = new String(ret).intern();

        initPeriod();
    }

    private void initPeriod() {
        period = new org.joda.time.Period("P" + (isPositive() ? periodString : periodString.substring(1)));
    }

    //TODO: remove Joda exposure / deprecated
    @Deprecated
    public org.joda.time.Period getJodaPeriod() {
        return period;
    }

    /**
     * Determines if the time period is positive.
     *
     * @return true if the period is positive, and false otherwise.
     */
    public boolean isPositive() {
        return periodString.charAt(0) != '-';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Period period1 = (Period) o;
        return periodString.equals(period1.periodString);
    }

    @Override
    public int hashCode() {
        return periodString.hashCode();
    }

    @Override
    public int compareTo(Period dateTime) {
        return periodString.compareTo(dateTime.periodString);
    }

    @Override
    public String toString() {
        return periodString;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initPeriod();
    }
}
