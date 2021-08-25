/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import org.joda.time.*;

import java.io.*;

public class DBPeriod implements Comparable<DBPeriod>, Serializable {

    private String periodString;

    private transient Period period = null;

    public DBPeriod() {
        // for serialization...
    }

    public DBPeriod(String periodString) {
        char ret[] = periodString.toCharArray();

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
        period = new Period("P" + (isPositive() ? periodString : periodString.substring(1)));
    }

    public Period getJodaPeriod() {
        return period;
    }

    public boolean isPositive() {
        return periodString.charAt(0) != '-';
    }

    public boolean equals(Object period) {
        return periodString.equals(((DBPeriod) period).periodString);
    }

    public int hashCode() {
        return periodString.hashCode();
    }

    public int compareTo(DBPeriod dateTime) {
        return periodString.compareTo(dateTime.periodString);
    }

    public String toString() {
        return periodString;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        initPeriod();
    }
}
