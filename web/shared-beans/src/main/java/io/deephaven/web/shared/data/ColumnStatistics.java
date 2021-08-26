package io.deephaven.web.shared.data;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A DTO containing the result data from a call to GenerateComparableStatsFunction or GenerateNumericalStatsFunction
 */
public class ColumnStatistics implements Serializable {
    public enum ColumnType {
        NUMERIC, COMPARABLE, DATETIME, NON_COMPARABLE,
    }

    private ColumnType type;
    private long size;
    private long count;

    // Data from a GenerateComparableStatsFunction
    private int numUnique;
    private String[] uniqueKeys;
    private long[] uniqueValues;

    // Data from a GenerateNumericalStatsFunction
    private double sum;
    private double absSum;
    private double min;
    private double max;
    private double absMin;
    private double absMax;

    // Data from a GenerateDBDateTimeStatsFunction
    private long minDateTime;
    private long maxDateTime;

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public long getSize() {
        return size;
    }

    public void setSize(final long size) {
        this.size = size;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public int getNumUnique() {
        return numUnique;
    }

    public void setNumUnique(int numUnique) {
        this.numUnique = numUnique;
    }

    public String[] getUniqueKeys() {
        return uniqueKeys;
    }

    public void setUniqueKeys(String[] uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
    }

    public long[] getUniqueValues() {
        return uniqueValues;
    }

    public void setUniqueValues(long[] uniqueValues) {
        this.uniqueValues = uniqueValues;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getAbsSum() {
        return absSum;
    }

    public void setAbsSum(double absSum) {
        this.absSum = absSum;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getAbsMin() {
        return absMin;
    }

    public void setAbsMin(double absMin) {
        this.absMin = absMin;
    }

    public double getAbsMax() {
        return absMax;
    }

    public void setAbsMax(double absMax) {
        this.absMax = absMax;
    }

    public long getMinDateTime() {
        return minDateTime;
    }

    public void setMinDateTime(final long minDateTime) {
        this.minDateTime = minDateTime;
    }

    public long getMaxDateTime() {
        return maxDateTime;
    }

    public void setMaxDateTime(final long maxDateTime) {
        this.maxDateTime = maxDateTime;
    }

    @Override
    public String toString() {
        return "ColumnStatistics{" +
                "type=" + type +
                ", size=" + size +
                ", count=" + count +
                ", numUnique=" + numUnique +
                ", uniqueKeys=" + Arrays.toString(uniqueKeys) +
                ", uniqueValues=" + Arrays.toString(uniqueValues) +
                ", sum=" + sum +
                ", absSum=" + absSum +
                ", min=" + min +
                ", max=" + max +
                ", absMin=" + absMin +
                ", absMax=" + absMax +
                ", minDateTime=" + minDateTime +
                ", maxDateTime=" + maxDateTime +
                '}';
    }
}
