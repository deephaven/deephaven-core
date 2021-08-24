package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.sources.chunk.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static io.deephaven.db.util.BooleanUtils.FALSE_BOOLEAN_AS_BYTE;
import static io.deephaven.db.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;
import static io.deephaven.db.util.BooleanUtils.TRUE_BOOLEAN_AS_BYTE;

public class ArrayGenerator {

    private static Map<Class, GeneratorWithSpaceSize> generatorWithSize = new ConcurrentHashMap<>();
    private static Map<Class, Function<Object, ArrayAdapter>> adapterBuilder =
        new ConcurrentHashMap<>();
    private static Map<Class, Function<Object, ChunkAdapter>> chunkAdapterBuilder =
        new ConcurrentHashMap<>();

    static {
        generatorWithSize.put(boolean.class, ArrayGenerator::randomBooleans);
        generatorWithSize.put(byte.class, ArrayGenerator::randomBytes);
        generatorWithSize.put(char.class, ArrayGenerator::randomChars);
        generatorWithSize.put(double.class, ArrayGenerator::randomDoubles);
        generatorWithSize.put(int.class, ArrayGenerator::randomInts);
        generatorWithSize.put(long.class, ArrayGenerator::randomLongs);
        generatorWithSize.put(float.class, ArrayGenerator::randomFloats);
        generatorWithSize.put(short.class, ArrayGenerator::randomShorts);
        generatorWithSize.put(String.class, ArrayGenerator::randomStrings);
    }

    static {
        adapterBuilder.put(boolean.class, BooleanArrayAdapter::new);
        adapterBuilder.put(byte.class, ByteArrayAdapter::new);
        adapterBuilder.put(char.class, CharacterArrayAdapter::new);
        adapterBuilder.put(double.class, DoubleArrayAdapter::new);
        adapterBuilder.put(float.class, FloatArrayAdapter::new);
        adapterBuilder.put(int.class, IntArrayAdapter::new);
        adapterBuilder.put(long.class, LongArrayAdapter::new);
        adapterBuilder.put(short.class, ShortArrayAdapter::new);
    }

    static {
        chunkAdapterBuilder.put(WritableBooleanChunk.class, BooleanChunkAdapter::new);
        chunkAdapterBuilder.put(WritableByteChunk.class, ByteChunkAdapter::new);
        chunkAdapterBuilder.put(WritableCharChunk.class, CharacterChunkAdapter::new);
        chunkAdapterBuilder.put(WritableDoubleChunk.class, DoubleChunkAdapter::new);
        chunkAdapterBuilder.put(WritableFloatChunk.class, FloatChunkAdapter::new);
        chunkAdapterBuilder.put(WritableIntChunk.class, IntChunkAdapter::new);
        chunkAdapterBuilder.put(WritableLongChunk.class, LongChunkAdapter::new);
        chunkAdapterBuilder.put(WritableShortChunk.class, ShortChunkAdapter::new);
    }

    public static byte[] randomBooleans(Random random, int size) {
        byte[] result = new byte[size];
        for (int i = 0; i < result.length; i++) {
            if (random.nextBoolean()) {
                result[i] = NULL_BOOLEAN_AS_BYTE;
            } else {
                result[i] = random.nextBoolean() ? TRUE_BOOLEAN_AS_BYTE : FALSE_BOOLEAN_AS_BYTE;
            }
        }
        return result;
    }

    public static Boolean[] randomBoxedBooleans(Random random, int size) {
        Boolean[] result = new Boolean[size];
        for (int i = 0; i < result.length; i++) {
            if (!random.nextBoolean()) {
                result[i] = random.nextBoolean();
            }
        }
        return result;
    }

    public static byte[] randomBytes(Random random, int size) {
        byte[] result = new byte[size];
        random.nextBytes(result);
        return result;
    }

    public static char[] randomChars(Random random, int size) {
        char[] result = new char[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = (char) random.nextInt();
        }
        return result;
    }

    public static double[] randomDoubles(Random random, int size) {
        double[] result = new double[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextDouble();
        }
        return result;
    }

    public static float[] randomFloats(Random random, int size) {
        float[] result = new float[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextFloat();
        }
        return result;
    }

    public static int[] randomInts(Random random, int size) {
        int[] result = new int[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextInt();
        }
        return result;
    }

    public static Object[] randomObjects(Random random, int size) {
        Object[] result = new Object[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = "" + random.nextInt();
        }
        return result;
    }

    public static short[] randomShorts(Random random, int size) {
        short[] result = new short[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = (short) random.nextInt();
        }
        return result;
    }

    public static long[] randomLongs(Random random, int size) {
        long[] result = new long[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextLong();
        }
        return result;
    }

    /**
     * @param objectType The type of the elements buit into the array
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static Object randomValues(Class objectType, Random random, int size, int spaceSize) {
        return generatorWithSize.get(objectType).generate(random, size, spaceSize);
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static boolean[] randomBooleans(Random random, int size, int spaceSize) {
        if (spaceSize > 1) {
            return randomBooleans(random, size, false, true);
        } else {
            return randomBooleans(random, size, false, false);
        }
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static byte[] randomBytes(Random random, int size, int spaceSize) {
        return randomBytes(random, size, (byte) 0, (byte) spaceSize);
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static char[] randomChars(Random random, int size, int spaceSize) {
        return randomChars(random, size, (char) 0, (char) spaceSize);
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static int[] randomInts(Random random, int size, int spaceSize) {
        return randomInts(random, size, 0, spaceSize);
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static long[] randomLongs(Random random, int size, int spaceSize) {
        return randomLongs(random, size, (long) 0, spaceSize);
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static short[] randomShorts(Random random, int size, int spaceSize) {
        return randomShorts(random, size, (short) 0, (short) spaceSize);
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static double[] randomDoubles(Random random, int size, int spaceSize) {
        return Arrays.stream(randomInts(random, size, 0, spaceSize)).asDoubleStream().toArray();
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static float[] randomFloats(Random random, int size, int spaceSize) {
        float[] result = new float[size];
        int[] randomInts = randomInts(random, size, 0, spaceSize);
        for (int i = 0; i < result.length; i++) {
            result[i] = randomInts[i];
        }
        return result;
    }

    /**
     * @param random An instance of the random number generator to be used
     * @param size The number of elements in the result
     * @param spaceSize The size of the random values domain (the number of distinct possible values
     *        that may exist in the result
     * @return An array with random values
     */
    public static String[] randomStrings(Random random, int size, int spaceSize) {
        return Arrays.stream(randomInts(random, size, 0, spaceSize)).mapToObj(Integer::toString)
            .toArray(String[]::new);
    }

    public static boolean[] randomBooleans(Random random, int size, boolean minValue,
        boolean maxValue) {
        boolean[] result = new boolean[size];
        if (minValue == maxValue) {
            Arrays.fill(result, minValue);
        } else {
            for (int i = 0; i < result.length; i++) {
                result[i] = random.nextBoolean();
            }
        }
        return result;
    }

    public static byte[] randomBytes(Random random, int size, byte minValue, byte maxValue) {
        byte[] result = new byte[size];
        if (maxValue <= 0) {
            maxValue = Byte.MAX_VALUE;
        }
        byte range = (byte) (maxValue - minValue);
        if (range <= 0) {
            range = Byte.MAX_VALUE;
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (random.nextInt(range) + minValue);
        }
        return result;
    }

    public static char[] randomChars(Random random, int size, char minValue, char maxValue) {
        char[] result = new char[size];
        if (maxValue <= 0) {
            maxValue = Character.MAX_VALUE;
        }
        char range = (char) (maxValue - minValue);
        if (range <= 0) {
            range = Character.MAX_VALUE;
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = (char) (random.nextInt(range) + minValue);
        }
        return result;
    }

    public static double[] randomDoubles(Random random, int size, double minValue,
        double maxValue) {
        double[] result = new double[size];
        if (maxValue <= 0) {
            maxValue = Double.MAX_VALUE;
        }
        double range = maxValue - minValue;
        if (range <= 0) {
            range = Double.MAX_VALUE;
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextDouble() * range + minValue;
        }
        return result;
    }

    public static float[] randomFloats(Random random, int size, float minValue, float maxValue) {
        float[] result = new float[size];
        if (maxValue <= 0) {
            maxValue = Float.MAX_VALUE;
        }
        float range = (maxValue - minValue);
        if (range <= 0) {
            range = Float.MAX_VALUE;
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextFloat() * range + minValue;
        }
        return result;
    }

    public static int[] randomInts(Random random, int size, int minValue, int maxValue) {
        int[] result = new int[size];
        if (maxValue <= 0) {
            maxValue = Integer.MAX_VALUE;
        }
        int range = maxValue - minValue;
        if (range <= 0) {
            range = Integer.MAX_VALUE;
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextInt(range) + minValue;
        }
        return result;
    }

    public static long[] randomLongs(Random random, int size, long minValue, long maxValue) {
        long[] result = new long[size];
        if (maxValue <= 0) {
            maxValue = Long.MAX_VALUE;
        }
        long range = maxValue - minValue;
        if (range <= 0) {
            range = Long.MAX_VALUE;
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = (Math.abs(random.nextLong()) % range) + minValue;
        }
        return result;
    }

    public static short[] randomShorts(Random random, int size, short minValue, short maxValue) {
        short[] result = new short[size];
        if (maxValue <= 0) {
            maxValue = Short.MAX_VALUE;
        }
        short range = (short) (maxValue - minValue);
        if (range <= 0) {
            range = Short.MAX_VALUE;
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = (short) Math.abs(((((short) random.nextInt()) % range) + minValue));
        }
        return result;
    }

    public static Object[] randomObjects(Random random, int size, int minValue, int maxValue) {
        Object[] result = new Object[size];
        if (maxValue <= 0) {
            maxValue = Integer.MAX_VALUE;
        }
        int range = maxValue - minValue;
        if (range <= 0) {
            range = Integer.MAX_VALUE;
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = "" + (random.nextInt(range) + minValue);
        }
        return result;
    }

    public static long[] indexDataGenerator(Random random, int size, double gapProbability,
        int maxStep, int maxValue) {
        long result[] = new long[size];
        if (size == 0) {
            return result;
        }
        result[0] =
            Math.max(0, Math.min(Math.abs(random.nextLong()), maxValue - (maxStep + 1) * size));


        for (int i = 1; i < result.length; i++) {
            long l = result[i];
            if (random.nextDouble() < gapProbability
                && result[i - 1] < maxValue - result.length + i - 2) {
                result[i] = result[i - 1] + 2 + random.nextInt(maxStep);
            } else {
                result[i] = result[i - 1] + 1;
            }
        }
        return result;
    }

    static public ArrayAdapter getAdapter(Object array) {
        return adapterBuilder
            .getOrDefault(array.getClass().getComponentType(), ObjectArrayAdapter::new)
            .apply(array);
    }

    @FunctionalInterface
    public interface GeneratorWithSpaceSize<ARRAY_T> {
        ARRAY_T generate(Random random, int size, int spaceSize);
    }

    public interface ArrayAdapter<T> {
        int length();

        T get(int i);

        void set(int i, T value);
    }

    static class ByteArrayAdapter implements ArrayAdapter<Byte> {

        private final byte[] array;

        ByteArrayAdapter(Object array) {
            this.array = (byte[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Byte get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, Byte value) {
            array[i] = value;
        }
    }

    static class BooleanArrayAdapter implements ArrayAdapter<Boolean> {

        private final boolean[] array;

        public BooleanArrayAdapter(Object array) {
            this.array = (boolean[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Boolean get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, Boolean value) {
            array[i] = value;
        }
    }

    static class CharacterArrayAdapter implements ArrayAdapter<Character> {

        private final char[] array;

        CharacterArrayAdapter(Object array) {
            this.array = (char[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Character get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, Character value) {
            array[i] = value;
        }
    }

    static class DoubleArrayAdapter implements ArrayAdapter<Double> {

        private final double[] array;

        DoubleArrayAdapter(Object array) {
            this.array = (double[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Double get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, Double value) {
            array[i] = value;
        }
    }

    static class FloatArrayAdapter implements ArrayAdapter<Float> {

        private final float[] array;

        FloatArrayAdapter(Object array) {
            this.array = (float[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Float get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, Float value) {
            array[i] = value;
        }
    }

    static class IntArrayAdapter implements ArrayAdapter<Integer> {

        private final int[] array;

        IntArrayAdapter(Object array) {
            this.array = (int[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Integer get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, Integer value) {
            array[i] = value;
        }
    }

    static class LongArrayAdapter implements ArrayAdapter<Long> {

        private final long[] array;

        LongArrayAdapter(Object array) {
            this.array = (long[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Long get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, Long value) {
            array[i] = value;
        }
    }

    static class ShortArrayAdapter implements ArrayAdapter<Short> {

        private final short[] array;

        ShortArrayAdapter(Object array) {
            this.array = (short[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Short get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, Short value) {
            array[i] = value;
        }
    }

    static class ObjectArrayAdapter<T> implements ArrayAdapter<T> {

        private final T[] array;

        ObjectArrayAdapter(Object array) {
            this.array = (T[]) array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public T get(int i) {
            return array[i];
        }

        @Override
        public void set(int i, T value) {
            array[i] = value;
        }
    }

    static public ChunkAdapter getChunkAdapter(Object chunk) {
        return chunkAdapterBuilder.getOrDefault(chunk.getClass(), ObjectChunkAdapter::new)
            .apply(chunk);
    }

    public interface ChunkAdapter<T> {
        int length();

        T get(int i);

        void set(int i, T value);
    }

    static class ByteChunkAdapter implements ChunkAdapter<Byte> {

        private final WritableByteChunk chunk;

        ByteChunkAdapter(Object chunk) {
            this.chunk = (WritableByteChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Byte get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Byte value) {
            chunk.set(i, value);
        }
    }

    static class BooleanChunkAdapter implements ChunkAdapter<Boolean> {

        private final WritableBooleanChunk chunk;

        public BooleanChunkAdapter(Object chunk) {
            this.chunk = (WritableBooleanChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Boolean get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Boolean value) {
            chunk.set(i, value);
        }
    }

    static class CharacterChunkAdapter implements ChunkAdapter<Character> {

        private final WritableCharChunk chunk;

        CharacterChunkAdapter(Object chunk) {
            this.chunk = (WritableCharChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Character get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Character value) {
            chunk.set(i, value);
        }
    }

    static class DoubleChunkAdapter implements ChunkAdapter<Double> {

        private final WritableDoubleChunk chunk;

        DoubleChunkAdapter(Object chunk) {
            this.chunk = (WritableDoubleChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Double get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Double value) {
            chunk.set(i, value);
        }
    }

    static class FloatChunkAdapter implements ChunkAdapter<Float> {

        private final WritableFloatChunk chunk;

        FloatChunkAdapter(Object chunk) {
            this.chunk = (WritableFloatChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Float get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Float value) {
            chunk.set(i, value);
        }
    }

    static class IntChunkAdapter implements ChunkAdapter<Integer> {

        private final WritableIntChunk chunk;

        IntChunkAdapter(Object chunk) {
            this.chunk = (WritableIntChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Integer get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Integer value) {
            chunk.set(i, value);
        }
    }

    static class LongChunkAdapter implements ChunkAdapter<Long> {

        private final WritableLongChunk chunk;

        LongChunkAdapter(Object chunk) {
            this.chunk = (WritableLongChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Long get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Long value) {
            chunk.set(i, value);
        }
    }

    static class ShortChunkAdapter implements ChunkAdapter<Short> {

        private final WritableShortChunk chunk;

        ShortChunkAdapter(Object chunk) {
            this.chunk = (WritableShortChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Short get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Short value) {
            chunk.set(i, value);
        }
    }

    static class ObjectChunkAdapter implements ChunkAdapter {

        private final WritableObjectChunk chunk;

        ObjectChunkAdapter(Object chunk) {
            this.chunk = (WritableObjectChunk) chunk;
        }

        @Override
        public int length() {
            return chunk.size();
        }

        @Override
        public Object get(int i) {
            return chunk.get(i);
        }

        @Override
        public void set(int i, Object value) {
            chunk.set(i, value);
        }
    }

}
