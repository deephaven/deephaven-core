package io.deephaven.engine.bench;

public class Functions {

    public static long identity(long ix) {
        return ix;
    }

    public static long mod_2(long ix) {
        return ix % 2;
    }

    public static long mod_10(long ix) {
        return ix % 10;
    }

    public static long mod_100(long ix) {
        return ix % 100;
    }

    public static long mod_1000(long ix) {
        return ix % 1000;
    }

    public static long div_2(long ix) {
        return ix / 2;
    }

    public static long div_10(long ix) {
        return ix / 10;
    }

    public static long div_100(long ix) {
        return ix / 100;
    }

    public static long div_1000(long ix) {
        return ix / 1000;
    }

    public static long prng(long ix) {
        // http://mostlymangling.blogspot.com/2019/12/stronger-better-morer-moremur-better.html
        ix ^= ix >>> 27;
        ix *= 0x3C79AC492BA7B653L;
        ix ^= ix >>> 33;
        ix *= 0x1C69B3F74AC4AE35L;
        ix ^= ix >>> 27;
        return ix;
    }

    public static long prng_2(long ix) {
        return (prng(ix) & 0x7FFFFFFFFFFFFFFFL) % 2;
    }

    public static long prng_10(long ix) {
        return (prng(ix) & 0x7FFFFFFFFFFFFFFFL) % 10;
    }

    public static long prng_100(long ix) {
        return (prng(ix) & 0x7FFFFFFFFFFFFFFFL) % 100;
    }

    public static long prng_1000(long ix) {
        return (prng(ix) & 0x7FFFFFFFFFFFFFFFL) % 1000;
    }

    /**
     * A binomial distribution with p=0.5, n=64.
     */
    public static long prng_binomial_64(long ix) {
        return Long.bitCount(prng(ix));
    }
}
