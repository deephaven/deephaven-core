package io.deephaven.csv;

import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class InferenceTest {

    @Test
    public void singleParserNoBackupNoItems() {
        noInfer(InferenceSpecs.builder().addParsers(Parser.INT).onNullParser(null).build());
    }

    @Test
    public void singleParserBackupNoItems() {
        infer(InferenceSpecs.builder().addParsers(Parser.INT).onNullParser(Parser.DOUBLE).build(), Parser.DOUBLE);
    }

    @Test
    public void singleParserNoBackupNullItems() {
        noInfer(InferenceSpecs.builder().addParsers(Parser.INT).onNullParser(null).build(), null, null);
    }

    @Test
    public void singleParserBackupNullItems() {
        infer(InferenceSpecs.builder().addParsers(Parser.INT).onNullParser(Parser.DOUBLE).build(), Parser.DOUBLE, null,
                null);
    }

    @Test
    public void noItems() {
        // all null defaults to string type
        infer(Parser.STRING);

        // if we use a custom inference specs with null parser, will not infer
        noInfer(InferenceSpecs.builder().addParsers(Parser.STRING).onNullParser(null).build());
    }

    @Test
    public void allNull() {
        // all null defaults to string type
        infer(Parser.STRING, null, null, null, null);

        // if we use a custom inference specs with null parser, will not infer
        noInfer(InferenceSpecs.builder().addParsers(Parser.STRING).onNullParser(null).build(), null, null, null, null);
    }

    @Test
    public void mixedType() {
        infer(Parser.STRING, "1.0", "1", null, "true", "False");
    }

    @Test
    public void stringType() {
        infer(Parser.STRING, "this", "should", null, "be", "a", "string");
    }

    @Test
    public void boolType() {
        infer(Parser.BOOL, "true", null, "True", "false", "False");
    }

    @Test
    public void notQuiteBool() {
        infer(Parser.STRING, "true", null, "1", "false", "False");
        infer(Parser.STRING, "true", null, "yes", "false", "False");
        infer(Parser.STRING, "true", null, "", "false", "False");
    }

    @Test
    public void charType() {
        infer(Parser.CHAR, "a", null, "b", "c", "c");
    }

    @Test
    public void notQuiteChar() {
        infer(Parser.STRING, "a", null, "", "c", "c");
        infer(Parser.STRING, "a", null, "bb", "c", "c");
    }

    @Test
    public void shortType() {
        infer(Parser.SHORT, "1", "2", null, "-1", String.valueOf(Short.MAX_VALUE));
    }

    @Test
    public void notQuiteShort() {
        infer(Parser.INT, "1", "2", null, "-1", String.valueOf(Short.MAX_VALUE + 1));
        infer(Parser.STRING, "1", "2", null, "-1", "");
    }

    @Test
    public void intType() {
        infer(Parser.INT, "1", "2", null, "-1", String.valueOf(Integer.MAX_VALUE));
    }

    @Test
    public void notQuiteInt() {
        infer(Parser.LONG, "1", "2", null, "-1", String.valueOf(Integer.MAX_VALUE + 1L));
        infer(Parser.STRING, "1", "2", null, "-1", "");
    }

    @Test
    public void longType() {
        infer(Parser.LONG, "1", "2", null, "-1", String.valueOf(Long.MAX_VALUE));
    }

    @Test
    public void notQuiteLong() {
        // one more than Long.MAX_VALUE
        infer(Parser.DOUBLE, "1", "2", null, "-1", "9223372036854775808");
        infer(Parser.STRING, "1", "2", null, "-1", "");
    }

    @Test
    public void doubleType() {
        infer(Parser.DOUBLE, "1", "1.1", null, "-1", String.valueOf(Double.MIN_VALUE));
        infer(Parser.DOUBLE, "1", "1.1", null, "-1", String.valueOf(Double.MAX_VALUE));
        infer(Parser.DOUBLE, "1", "1.1", null, "-1", String.valueOf(Double.NEGATIVE_INFINITY));
        infer(Parser.DOUBLE, "1", "1.1", null, "-1", String.valueOf(Double.POSITIVE_INFINITY));
        infer(Parser.DOUBLE, "1", "1.1", null, "-1", String.valueOf(Double.NaN));
        infer(Parser.DOUBLE, "1", "1.1", null, "-1", String.valueOf(Double.MIN_NORMAL));
    }

    @Test
    public void notQuiteDouble() {
        infer(Parser.STRING, "1", "1.1", null, "-1", "");
    }

    @Test
    public void instantType() {
        infer(Parser.INSTANT, "2019-08-25T11:34:56.000Z", null, "2021-01-01T09:00:00Z");
    }

    @Test
    public void notQuiteInstant() {
        infer(Parser.STRING, "2019-08-25T11:34:56.000Z", null, "");
    }

    @Test
    public void shortCircuitEvenIfEventuallyIncorrect() {
        final InferenceSpecs shortOrInt = InferenceSpecs.builder().addParsers(Parser.SHORT, Parser.INT).build();
        infer(shortOrInt, Parser.INT, "1", "2", Integer.toString(Short.MAX_VALUE + 1), "This is not an int");
    }

    private static void infer(Parser<?> expected, String... values) {
        infer(InferenceSpecs.standard(), expected, values);
    }

    private static void noInfer(String... values) {
        noInfer(InferenceSpecs.standard(), values);
    }

    private static void infer(InferenceSpecs specs, Parser<?> expected, String... values) {
        assertThat(specs.infer(Arrays.asList(values).iterator())).contains(expected);
    }

    private static void noInfer(InferenceSpecs specs, String... values) {
        assertThat(specs.infer(Arrays.asList(values).iterator())).isEmpty();
    }
}
