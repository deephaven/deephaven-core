package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;

class WeightedAverageOperator {
    interface Operator {
        State getState(long resultKey);

        Class<?> getResultType();

        void setDestination(WritableColumnSource<Double> columnSource);
    }

    interface State {
        void addValue(long key);

        void addPrevValue(long key);

        void removeValue(long prevKey);

        void updateResult();
    }

    @SuppressWarnings("unchecked")
    static <C, W> Operator getOperator(ColumnSource<C> components, ColumnSource<W> weights) {
        Class<C> componentType = (Class<C>) io.deephaven.util.type.TypeUtils.getBoxedType(components.getType());
        Class<W> weightType = (Class<W>) TypeUtils.getBoxedType(weights.getType());

        if (componentType == Double.class)
            return getDoubleOperator(weightType, (ColumnSource<Double>) components, weights);
        if (componentType == Float.class)
            return getFloatOperator(weightType, (ColumnSource<Float>) components, weights);
        if (componentType == Character.class)
            return getCharOperator(weightType, (ColumnSource<Character>) components, weights);
        if (componentType == Byte.class)
            return getByteOperator(weightType, (ColumnSource<Byte>) components, weights);
        if (componentType == Short.class)
            return getShortOperator(weightType, (ColumnSource<Short>) components, weights);
        if (componentType == Integer.class)
            return getIntegerOperator(weightType, (ColumnSource<Integer>) components, weights);
        if (componentType == Long.class)
            return getLongOperator(weightType, (ColumnSource<Long>) components, weights);

        throw new UnsupportedOperationException(
                "Can not perform a weighted average with component type: " + componentType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator getDoubleOperator(Class<?> weightType, ColumnSource<Double> components,
            ColumnSource weights) {
        if (weightType == Double.class)
            return new OperatorImpl(new DoubleGetter(components), new DoubleGetter(weights));
        if (weightType == Float.class)
            return new OperatorImpl(new DoubleGetter(components), new FloatGetter(weights));
        if (weightType == Character.class)
            return new OperatorImpl(new DoubleGetter(components), new CharGetter(weights));
        if (weightType == Byte.class)
            return new OperatorImpl(new DoubleGetter(components), new ByteGetter(weights));
        if (weightType == Short.class)
            return new OperatorImpl(new DoubleGetter(components), new ShortGetter(weights));
        if (weightType == Integer.class)
            return new OperatorImpl(new DoubleGetter(components), new IntegerGetter(weights));
        if (weightType == Long.class)
            return new OperatorImpl(new DoubleGetter(components), new LongGetter(weights));

        throw new UnsupportedOperationException("Can not perform a weighted average with weight type: " + weightType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator getFloatOperator(Class<?> weightType, ColumnSource<Float> components,
            ColumnSource weights) {
        if (weightType == Double.class)
            return new OperatorImpl(new FloatGetter(components), new DoubleGetter(weights));
        if (weightType == Float.class)
            return new OperatorImpl(new FloatGetter(components), new FloatGetter(weights));
        if (weightType == Character.class)
            return new OperatorImpl(new FloatGetter(components), new CharGetter(weights));
        if (weightType == Byte.class)
            return new OperatorImpl(new FloatGetter(components), new ByteGetter(weights));
        if (weightType == Short.class)
            return new OperatorImpl(new FloatGetter(components), new ShortGetter(weights));
        if (weightType == Integer.class)
            return new OperatorImpl(new FloatGetter(components), new IntegerGetter(weights));
        if (weightType == Long.class)
            return new OperatorImpl(new FloatGetter(components), new LongGetter(weights));

        throw new UnsupportedOperationException("Can not perform a weighted average with weight type: " + weightType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator getCharOperator(Class<?> weightType, ColumnSource<Character> components,
            ColumnSource weights) {
        if (weightType == Double.class)
            return new OperatorImpl(new CharGetter(components), new DoubleGetter(weights));
        if (weightType == Float.class)
            return new OperatorImpl(new CharGetter(components), new FloatGetter(weights));
        if (weightType == Character.class)
            return new OperatorImpl(new CharGetter(components), new CharGetter(weights));
        if (weightType == Byte.class)
            return new OperatorImpl(new CharGetter(components), new ByteGetter(weights));
        if (weightType == Short.class)
            return new OperatorImpl(new CharGetter(components), new ShortGetter(weights));
        if (weightType == Integer.class)
            return new OperatorImpl(new CharGetter(components), new IntegerGetter(weights));
        if (weightType == Long.class)
            return new OperatorImpl(new CharGetter(components), new LongGetter(weights));

        throw new UnsupportedOperationException("Can not perform a weighted average with weight type: " + weightType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator getByteOperator(Class<?> weightType, ColumnSource<Byte> components, ColumnSource weights) {
        if (weightType == Double.class)
            return new OperatorImpl(new ByteGetter(components), new DoubleGetter(weights));
        if (weightType == Float.class)
            return new OperatorImpl(new ByteGetter(components), new FloatGetter(weights));
        if (weightType == Character.class)
            return new OperatorImpl(new ByteGetter(components), new CharGetter(weights));
        if (weightType == Byte.class)
            return new OperatorImpl(new ByteGetter(components), new ByteGetter(weights));
        if (weightType == Short.class)
            return new OperatorImpl(new ByteGetter(components), new ShortGetter(weights));
        if (weightType == Integer.class)
            return new OperatorImpl(new ByteGetter(components), new IntegerGetter(weights));
        if (weightType == Long.class)
            return new OperatorImpl(new ByteGetter(components), new LongGetter(weights));

        throw new UnsupportedOperationException("Can not perform a weighted average with weight type: " + weightType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator getShortOperator(Class<?> weightType, ColumnSource<Short> components,
            ColumnSource weights) {
        if (weightType == Double.class)
            return new OperatorImpl(new ShortGetter(components), new DoubleGetter(weights));
        if (weightType == Float.class)
            return new OperatorImpl(new ShortGetter(components), new FloatGetter(weights));
        if (weightType == Character.class)
            return new OperatorImpl(new ShortGetter(components), new CharGetter(weights));
        if (weightType == Byte.class)
            return new OperatorImpl(new ShortGetter(components), new ByteGetter(weights));
        if (weightType == Short.class)
            return new OperatorImpl(new ShortGetter(components), new ShortGetter(weights));
        if (weightType == Integer.class)
            return new OperatorImpl(new ShortGetter(components), new IntegerGetter(weights));
        if (weightType == Long.class)
            return new OperatorImpl(new ShortGetter(components), new LongGetter(weights));

        throw new UnsupportedOperationException("Can not perform a weighted average with weight type: " + weightType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator getIntegerOperator(Class<?> weightType, ColumnSource<Integer> components,
            ColumnSource weights) {
        if (weightType == Double.class)
            return new OperatorImpl(new IntegerGetter(components), new DoubleGetter(weights));
        if (weightType == Float.class)
            return new OperatorImpl(new IntegerGetter(components), new FloatGetter(weights));
        if (weightType == Character.class)
            return new OperatorImpl(new IntegerGetter(components), new CharGetter(weights));
        if (weightType == Byte.class)
            return new OperatorImpl(new IntegerGetter(components), new ByteGetter(weights));
        if (weightType == Short.class)
            return new OperatorImpl(new IntegerGetter(components), new ShortGetter(weights));
        if (weightType == Integer.class)
            return new OperatorImpl(new IntegerGetter(components), new IntegerGetter(weights));
        if (weightType == Long.class)
            return new OperatorImpl(new IntegerGetter(components), new LongGetter(weights));

        throw new UnsupportedOperationException("Can not perform a weighted average with weight type: " + weightType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator getLongOperator(Class<?> weightType, ColumnSource<Long> components, ColumnSource weights) {
        if (weightType == Double.class)
            return new OperatorImpl(new LongGetter(components), new DoubleGetter(weights));
        if (weightType == Float.class)
            return new OperatorImpl(new LongGetter(components), new FloatGetter(weights));
        if (weightType == Character.class)
            return new OperatorImpl(new LongGetter(components), new CharGetter(weights));
        if (weightType == Byte.class)
            return new OperatorImpl(new LongGetter(components), new ByteGetter(weights));
        if (weightType == Short.class)
            return new OperatorImpl(new LongGetter(components), new ShortGetter(weights));
        if (weightType == Integer.class)
            return new OperatorImpl(new LongGetter(components), new IntegerGetter(weights));
        if (weightType == Long.class)
            return new OperatorImpl(new LongGetter(components), new LongGetter(weights));

        throw new UnsupportedOperationException("Can not perform a weighted average with weight type: " + weightType);
    }

    private interface ValueGetter {
        double get(long key);

        double getPrev(long key);
    }

    private static class OperatorImpl implements Operator {
        private WritableColumnSource<Double> dest;
        private final ValueGetter componentGetter;
        private final ValueGetter weightGetter;

        OperatorImpl(ValueGetter componentGetter, ValueGetter weightGetter) {
            this.componentGetter = componentGetter;
            this.weightGetter = weightGetter;
        }

        @Override
        public State getState(long resultKey) {
            return new State(resultKey);
        }

        @Override
        public Class<?> getResultType() {
            return double.class;
        }

        @Override
        public void setDestination(WritableColumnSource<Double> dest) {
            this.dest = dest;
        }

        private class State implements WeightedAverageOperator.State {
            double weightedSum;
            double sumOfWeights;
            double nanCount;
            double nonNullCount;
            private final long resultIndex;

            State(long resultIndex) {
                this.resultIndex = resultIndex;
            }

            @Override
            public void addValue(long key) {
                doAdd(componentGetter.get(key), weightGetter.get(key));
            }

            @Override
            public void addPrevValue(long key) {
                doAdd(componentGetter.getPrev(key), weightGetter.getPrev(key));
            }

            private void doAdd(double component, double weight) {
                if (Double.isNaN(component) || Double.isNaN(weight)) {
                    nanCount++;
                    return;
                }
                if (component == QueryConstants.NULL_DOUBLE || weight == QueryConstants.NULL_DOUBLE) {
                    return;
                }
                weightedSum += (component * weight);
                sumOfWeights += weight;
                nonNullCount++;
            }

            @Override
            public void removeValue(long key) {
                final double component = componentGetter.getPrev(key);
                final double weight = weightGetter.getPrev(key);

                if (Double.isNaN(component) || Double.isNaN(weight)) {
                    nanCount--;
                    return;
                }
                if (component == QueryConstants.NULL_DOUBLE || weight == QueryConstants.NULL_DOUBLE) {
                    return;
                }
                weightedSum -= (component * weight);
                sumOfWeights -= weight;
                nonNullCount--;
            }

            private double getResult() {
                if (nanCount > 0)
                    return Double.NaN;
                if (nonNullCount == 0)
                    return QueryConstants.NULL_DOUBLE;
                return weightedSum / sumOfWeights;
            }

            @Override
            public void updateResult() {
                dest.set(resultIndex, getResult());
            }
        }
    }

    private static class DoubleGetter implements ValueGetter {
        private final ColumnSource<Double> columnSource;

        private DoubleGetter(ColumnSource<Double> columnSource) {
            this.columnSource = columnSource;
        }

        @Override
        public double get(long key) {
            return columnSource.getDouble(key);
        }

        @Override
        public double getPrev(long key) {
            return columnSource.getPrevDouble(key);
        }
    }

    private static class FloatGetter implements ValueGetter {
        private final ColumnSource<Float> columnSource;

        private FloatGetter(ColumnSource<Float> columnSource) {
            this.columnSource = columnSource;
        }

        @Override
        public double get(long key) {
            final float aFloat = columnSource.getFloat(key);
            return aFloat == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : aFloat;
        }

        @Override
        public double getPrev(long key) {
            final float aFloat = columnSource.getPrevFloat(key);
            return aFloat == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : aFloat;
        }
    }

    private static class CharGetter implements ValueGetter {
        private final ColumnSource<Character> columnSource;

        private CharGetter(ColumnSource<Character> columnSource) {
            this.columnSource = columnSource;
        }

        @Override
        public double get(long key) {
            final char aChar = columnSource.getChar(key);
            return aChar == QueryConstants.NULL_CHAR ? QueryConstants.NULL_DOUBLE : aChar;
        }

        @Override
        public double getPrev(long key) {
            final char aChar = columnSource.getPrevChar(key);
            return aChar == QueryConstants.NULL_CHAR ? QueryConstants.NULL_DOUBLE : aChar;
        }
    }

    private static class ByteGetter implements ValueGetter {
        private final ColumnSource<Byte> columnSource;

        private ByteGetter(ColumnSource<Byte> columnSource) {
            this.columnSource = columnSource;
        }

        @Override
        public double get(long key) {
            final byte aByte = columnSource.getByte(key);
            return aByte == QueryConstants.NULL_BYTE ? QueryConstants.NULL_DOUBLE : aByte;
        }

        @Override
        public double getPrev(long key) {
            final byte aByte = columnSource.getPrevByte(key);
            return aByte == QueryConstants.NULL_BYTE ? QueryConstants.NULL_DOUBLE : aByte;
        }
    }

    private static class ShortGetter implements ValueGetter {
        private final ColumnSource<Short> columnSource;

        private ShortGetter(ColumnSource<Short> columnSource) {
            this.columnSource = columnSource;
        }

        @Override
        public double get(long key) {
            final short aShort = columnSource.getShort(key);
            return aShort == QueryConstants.NULL_SHORT ? QueryConstants.NULL_DOUBLE : aShort;
        }

        @Override
        public double getPrev(long key) {
            final short aShort = columnSource.getPrevShort(key);
            return aShort == QueryConstants.NULL_SHORT ? QueryConstants.NULL_DOUBLE : aShort;
        }
    }

    private static class IntegerGetter implements ValueGetter {
        private final ColumnSource<Integer> columnSource;

        private IntegerGetter(ColumnSource<Integer> columnSource) {
            this.columnSource = columnSource;
        }

        @Override
        public double get(long key) {
            final int anInt = columnSource.getInt(key);
            return anInt == QueryConstants.NULL_INT ? QueryConstants.NULL_DOUBLE : anInt;
        }

        @Override
        public double getPrev(long key) {
            final int anInt = columnSource.getPrevInt(key);
            return anInt == QueryConstants.NULL_INT ? QueryConstants.NULL_DOUBLE : anInt;
        }
    }

    private static class LongGetter implements ValueGetter {
        private final ColumnSource<Long> columnSource;

        private LongGetter(ColumnSource<Long> columnSource) {
            this.columnSource = columnSource;
        }

        @Override
        public double get(long key) {
            final long aLong = columnSource.getLong(key);
            return aLong == QueryConstants.NULL_INT ? QueryConstants.NULL_DOUBLE : aLong;
        }

        @Override
        public double getPrev(long key) {
            final long aLong = columnSource.getPrevLong(key);
            return aLong == QueryConstants.NULL_INT ? QueryConstants.NULL_DOUBLE : aLong;
        }
    }
}
