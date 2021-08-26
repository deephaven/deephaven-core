package io.deephaven.db.v2.select.chunkfilters;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.db.v2.select.CharRangeFilter;
import io.deephaven.db.v2.select.FloatRangeFilter;

import java.io.IOException;
import java.util.Collections;

public class ReplicateChunkFilters {
    public static void main(String[] args) throws IOException {

        ReplicatePrimitiveCode.charToShortAndByte(CharRangeComparator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToInteger(CharRangeComparator.class, ReplicatePrimitiveCode.MAIN_SRC,
                Collections.emptyMap());

        ReplicatePrimitiveCode.charToShortAndByte(CharRangeFilter.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToInteger(CharRangeFilter.class, ReplicatePrimitiveCode.MAIN_SRC,
                Collections.emptyMap());
        ReplicatePrimitiveCode.charToLong(CharRangeFilter.class, ReplicatePrimitiveCode.MAIN_SRC);

        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatRangeComparator.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatRangeFilter.class, ReplicatePrimitiveCode.MAIN_SRC);

        ReplicatePrimitiveCode.charToAllButBoolean(CharChunkMatchFilterFactory.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
