package io.deephaven.db.v2.parquet;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;

public abstract class DictionaryAdapter<RESULT_ARRAY> {

    static DictionaryAdapter getAdapter(Dictionary dictionary, Object nullValue) {
        if (dictionary instanceof PlainValuesDictionary.PlainLongDictionary) {
            return new LongAdapter((PlainValuesDictionary.PlainLongDictionary) dictionary,
                nullValue);
        } else if (dictionary instanceof PlainValuesDictionary.PlainFloatDictionary) {
            return new FloatAdapter((PlainValuesDictionary.PlainFloatDictionary) dictionary,
                nullValue);
        } else if (dictionary instanceof PlainValuesDictionary.PlainIntegerDictionary) {
            return new IntegerAdapter((PlainValuesDictionary.PlainIntegerDictionary) dictionary,
                nullValue);
        } else if (dictionary instanceof PlainValuesDictionary.PlainDoubleDictionary) {
            return new DoubleAdapter((PlainValuesDictionary.PlainDoubleDictionary) dictionary,
                nullValue);
        } else if (dictionary instanceof PlainValuesDictionary.PlainBinaryDictionary) {
            return new BinaryAdapter((PlainValuesDictionary.PlainBinaryDictionary) dictionary,
                nullValue);
        }
        throw new UnsupportedOperationException(
            "No adapter available for " + dictionary.getClass().getSimpleName());
    }

    public abstract void apply(RESULT_ARRAY result, int destIndex, int keyIndex);

    public abstract RESULT_ARRAY createResult(int numValues);

    private static class LongAdapter extends DictionaryAdapter<long[]> {
        private final long[] dictionaryMapping;

        public LongAdapter(PlainValuesDictionary.PlainLongDictionary dictionary, Object nullValue) {
            super();
            dictionaryMapping = new long[dictionary.getMaxId() + 2];
            for (int i = 0; i < dictionaryMapping.length - 1; i++) {
                dictionaryMapping[i] = dictionary.decodeToLong(i);
            }
            dictionaryMapping[dictionary.getMaxId() + 1] = (Long) nullValue;
        }

        @Override
        public void apply(long[] result, int destIndex, int keyIndex) {
            result[destIndex] = dictionaryMapping[keyIndex];
        }

        @Override
        public long[] createResult(int numValues) {
            return new long[numValues];
        }
    }

    private static class FloatAdapter extends DictionaryAdapter<float[]> {
        private final float[] dictionaryMapping;

        public FloatAdapter(PlainValuesDictionary.PlainFloatDictionary dictionary,
            Object nullValue) {
            super();
            dictionaryMapping = new float[dictionary.getMaxId() + 2];
            for (int i = 0; i < dictionaryMapping.length - 1; i++) {
                dictionaryMapping[i] = dictionary.decodeToFloat(i);
            }
            dictionaryMapping[dictionary.getMaxId() + 1] = (Float) nullValue;
        }

        @Override
        public void apply(float[] result, int destIndex, int keyIndex) {
            result[destIndex] = dictionaryMapping[keyIndex];
        }

        @Override
        public float[] createResult(int numValues) {
            return new float[numValues];
        }
    }

    private static class IntegerAdapter extends DictionaryAdapter<int[]> {
        private final int[] dictionaryMapping;

        public IntegerAdapter(PlainValuesDictionary.PlainIntegerDictionary dictionary,
            Object nullValue) {
            super();
            dictionaryMapping = new int[dictionary.getMaxId() + 2];
            for (int i = 0; i < dictionaryMapping.length - 1; i++) {
                dictionaryMapping[i] = dictionary.decodeToInt(i);
            }
            dictionaryMapping[dictionary.getMaxId() + 1] = (Integer) nullValue;
        }

        @Override
        public void apply(int[] result, int destIndex, int keyIndex) {
            result[destIndex] = dictionaryMapping[keyIndex];
        }

        @Override
        public int[] createResult(int numValues) {
            return new int[numValues];
        }
    }

    private static class DoubleAdapter extends DictionaryAdapter<double[]> {
        private final double[] dictionaryMapping;

        public DoubleAdapter(PlainValuesDictionary.PlainDoubleDictionary dictionary,
            Object nullValue) {
            super();
            dictionaryMapping = new double[dictionary.getMaxId() + 2];
            for (int i = 0; i < dictionaryMapping.length - 1; i++) {
                dictionaryMapping[i] = dictionary.decodeToDouble(i);
            }
            dictionaryMapping[dictionary.getMaxId() + 1] = (Double) nullValue;
        }

        @Override
        public void apply(double[] result, int destIndex, int keyIndex) {
            result[destIndex] = dictionaryMapping[keyIndex];
        }

        @Override
        public double[] createResult(int numValues) {
            return new double[numValues];
        }
    }

    private static class BinaryAdapter extends DictionaryAdapter<String[]> {
        private final String[] dictionaryMapping;

        public BinaryAdapter(PlainValuesDictionary.PlainBinaryDictionary dictionary,
            Object nullValue) {
            super();
            dictionaryMapping = new String[dictionary.getMaxId() + 2];
            for (int i = 0; i < dictionaryMapping.length - 1; i++) {
                dictionaryMapping[i] = dictionary.decodeToBinary(i).toStringUsingUTF8();
            }
            dictionaryMapping[dictionary.getMaxId() + 1] = (String) nullValue;
        }

        @Override
        public void apply(String[] result, int destIndex, int keyIndex) {
            result[destIndex] = dictionaryMapping[keyIndex];
        }

        @Override
        public String[] createResult(int numValues) {
            return new String[numValues];
        }
    }


}

