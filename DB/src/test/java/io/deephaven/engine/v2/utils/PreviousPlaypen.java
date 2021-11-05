package io.deephaven.engine.v2.utils;

import io.deephaven.engine.v2.sources.LogicalClock;

public class PreviousPlaypen {
    public static void main(String[] args) {
        // LogicalClock.DEFAULT.startUpdateCycle();

        // final TrackingMutableRowSet i = RowSetFactory.flat(100).toTracking();;
        // System.out.println(i);
        // System.out.println(i.getPrevRowSet());
        //
        // LogicalClock.DEFAULT.completeUpdateCycle();
        //
        // System.out.println(i);
        // System.out.println(i.getPrevRowSet());


        LogicalClock.DEFAULT.startUpdateCycle();

        final TrackingMutableRowSet i2 = RowSetFactory.flat(200).toTracking();
        System.out.println(i2);
        System.out.println(i2.getPrevRowSet());

        i2.insert(500);

        System.out.println(i2);
        System.out.println(i2.getPrevRowSet());

        LogicalClock.DEFAULT.completeUpdateCycle();

        System.out.println(i2);
        System.out.println(i2.getPrevRowSet());

    }
}
