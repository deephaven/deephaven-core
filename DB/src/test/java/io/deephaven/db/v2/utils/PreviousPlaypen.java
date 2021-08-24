package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.LogicalClock;

public class PreviousPlaypen {
    public static void main(String[] args) {
        // LogicalClock.DEFAULT.startUpdateCycle();

        // final Index i = Index.FACTORY.getFlatIndex(100);
        // System.out.println(i);
        // System.out.println(i.getPrevIndex());
        //
        // LogicalClock.DEFAULT.completeUpdateCycle();
        //
        // System.out.println(i);
        // System.out.println(i.getPrevIndex());


        LogicalClock.DEFAULT.startUpdateCycle();

        final Index i2 = Index.FACTORY.getFlatIndex(200);
        System.out.println(i2);
        System.out.println(i2.getPrevIndex());

        i2.insert(500);

        System.out.println(i2);
        System.out.println(i2.getPrevIndex());

        LogicalClock.DEFAULT.completeUpdateCycle();

        System.out.println(i2);
        System.out.println(i2.getPrevIndex());

    }
}
